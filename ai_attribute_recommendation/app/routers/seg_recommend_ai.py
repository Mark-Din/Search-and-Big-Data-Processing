import os
import json
import datetime
from typing import Optional
import numpy as np
import pandas as pd
import psycopg2
import dspy
import global_vars as global_vars
import torch
import asyncio
import hashlib
from concurrent.futures import ThreadPoolExecutor

from prompts import prompt_attribute, system_message_attribute, prompt_explanation, system_message_explanation
from common.init_log import init_log
from database import Database

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
# from performance_test import profile_function


router = APIRouter(prefix="/seg_ai", tags=["Segment Recommendation AI"])

logger = init_log("attribute_recommendations")

cached_data = None  # Store loaded JSON
client_stop_flags = {}

model_name = "wangrongsheng/mistral-7b-v0.3-chinese:latest"

def generate_embedding(text):
    """Generate embeddings using Hugging Face transformer model."""
    tokenizer = global_vars.tokenizer  # Get tokenizer from global storage
    model = global_vars.model  # Get model from global storage

    """Generate embeddings using Hugging Face transformer model."""
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=512)
    with torch.no_grad():
        embedding = model(**inputs).last_hidden_state.mean(dim=1).squeeze().numpy()
    return embedding

def preprocess_json(file_path: str) -> dict:
    global cached_data

    if cached_data is None:
        """Load and preprocess JSON data."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading JSON file: {e}", exc_info=True)
            return {}
    return cached_data
    

def store_embedding(conn, user_id, user_name, segment_name, answer, name_embedding, data_embedding, score) -> int:

    if not conn:
        logger.error("Database connection is not established.")
        conn = Database.get_instance("ai")

    cur = conn.cursor()

    try:
        """ Step 1: Insert user if not exists """
        cur.execute("""
            INSERT INTO users (user_id, user_name)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO NOTHING;
        """, (user_id, user_name))

        """ Step 2: Insert segment if not exists """
        cur.execute("""
            INSERT INTO segments (segment_name, embedding_segment)
            VALUES (%s, %s)
            ON CONFLICT (segment_name) DO NOTHING;
        """, (segment_name, name_embedding))

        """ Step 3: Get segment_id for foreign key reference """
        cur.execute("SELECT id FROM segments WHERE segment_name = %s;", (segment_name,))
        segment_id = cur.fetchone()[0]  # Fetch segment_id

        """ Step 4: Insert recommendation """
        cur.execute("""
            INSERT INTO recommendations (user_id, segment_id, recommend_json_ml, embedding_recommend_ml, eval_score)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
        """, (user_id, segment_id, answer, data_embedding, score))

        recommendation_id = cur.fetchone()[0]  # Fetch the new recommendation ID

        conn.commit()
        return recommendation_id
    except psycopg2.Error as e:
        logger.error(f"Error inserting data into database: {e}", exc_info=True)
        conn.rollback()
        return None

def search_similar_segments(conn, query_embedding, threshold=0.2):
    if not conn:
        logger.error("Database connection is not established.")
        conn = Database.get_instance("ai")
    cur = conn.cursor()
    
    """ Search for the most similar segment using pgvector """
    cur.execute("""
        SELECT id, embedding_segment <-> %s as distance
        FROM segments
        where embedding_segment <-> %s < %s
        ORDER BY distance
        LIMIT 1;
    """, (query_embedding, query_embedding, threshold))
    
    segment_result = cur.fetchone()
    if not segment_result:
        return None  # No similar segments found
    print(f"segment_result: {segment_result}")
    segment_id = segment_result[0]  # Unpack values

    """ Now fetch the recommendation_id from recommendations using segment_id """
    cur.execute("SELECT id FROM recommendations WHERE segment_id = %s ORDER BY created_at DESC LIMIT 3;", (segment_id,))
    recommendation_result = cur.fetchall()

    if not recommendation_result:
        return None  # No recommendations found for this segment

    # Collect recommendation IDs
    recommendation_ids = [r[0] for r in recommendation_result]

    """ Fetch user-modified recommendation first (if exists) """
    cur.execute("SELECT recommendation_id, recommend_json_user FROM user_recommendations WHERE recommendation_id in %s LIMIT 3;", (tuple(recommendation_ids),))
    answers = cur.fetchall()
    if not answers:  # If no user-modified recommendation, fetch the ML-generated one
        cur.execute("SELECT id, recommend_json_ml FROM recommendations WHERE id in %s ORDER BY eval_score DESC LIMIT 3;", (tuple(recommendation_ids),))
        answers = cur.fetchall()

    else:
        if len(answers) != 3:
            # If not enough user-modified recommendations, fetch the ML-generated ones to fill the gap
            remaining_count = 3 - len(answers)
            cur.execute("SELECT id, recommend_json_ml FROM recommendations WHERE id in %s ORDER BY eval_score DESC LIMIT %s;", (tuple(recommendation_ids), remaining_count))
            additional_answers = cur.fetchall()
            answers.extend(additional_answers)

    return answers


def add_dynamic_instructions(segment_name):
    if any(keyword in segment_name for keyword in ["VIP", "高端", "貴賓"]):
        return "This is a high-value segment. Prioritize high spending, frequent purchases, and exclusive product preferences."
    elif any(keyword in segment_name for keyword in ["金融", "投資", "ETF"]):
        return "This is a financial-related segment. Emphasize financial attributes such as ETF involvement and transaction frequency."
    elif any(keyword in segment_name for keyword in ["品牌大使", "代言人"]):
        return "This segment focuses on brand representation. Prioritize high consumption, marketing roles, and frequent purchases."
    elif any(keyword in segment_name for keyword in ["行銷", "Marketing"]):
        return "This segment targets marketing professionals. Highlight marketing roles, creativity, and related consumption preferences."
    else:
        return "This is a general segment. Prioritize balanced attributes across all categories."


def generate_recommendations(segment_name, categories, category, client_id):

    # client = global_vars.client  # Get Ollama client from global storage
    client = global_vars.client

    """Use Ollama to generate attribute recommendations."""

    # dynamic_instructions = add_dynamic_instructions(segment_name)

    prompt = prompt_attribute.format(
        dynamic_instructions = '',#dynamic_instructions, 
        segment_name=segment_name, 
        attribute_explanations = '',# In case we need attribute_explanations to reinforce the recommendation, now we don't need it
        categories_dict=categories
        )

    retry = 0
    while retry < 3:
        # Check for stop sigal 
        if client_stop_flags.get(client_id, False):
            print(f"[STOPPED] client_id={client_id}")
            return None
        
        logger.debug('Start generating recommendations...')
        response = client.chat(model=model_name, 
        messages=[
            {"role": "system", "content": system_message_attribute},
            {"role": "user", "content": prompt}
            ],
            format="json"
        )
        logger.debug('Finish generating recommendations...')
        answer = response["message"]["content"]
    
        try:
            ai_response = json.loads(answer)
            logger.debug(f"Response: {ai_response}")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
            continue

        if category in ai_response['selected_attributes'] and segment_name == ai_response['segment']:
            return ai_response 
        
        retry += 1
    else:
        return None


def generate_explanation(whole_category):
    client = global_vars.client  # Get Ollama client from global storage

    """Use Ollama to generate attribute explanations."""
    prompt_ = prompt_explanation.format(
        categories_dict = whole_category
    )

    retry = 0
    while retry < 3:
        response = client.chat(model=model_name, 
            messages=[
                {"role": "system", "content": system_message_explanation},
                {"role": "user", "content": prompt_}
                ],
                format="json"
            )
        retry += 1
        if "explanation" in response["message"]["content"] and 'better_segment_name' in response["message"]["content"]:
            return response["message"]["content"]
    else:
        return None


@router.get("/init_database", response_class=JSONResponse)
def init_database():
    print("Starting application...")
    # Initialize database connection and vector table
    conn = Database.connect("ai")
    if not conn:
        logger.error("Failed to connect to database.")
        return JSONResponse(content={"response": "Failed to connect to database."}, status_code=500)
    Database.init_tables(conn)
    conn.close() 

    return JSONResponse(content={"response": "Database initialized."}, status_code=200)


@router.get('/filtered_attributes', response_class=JSONResponse)
def get_filtered_attributes():
    try:
        con = Database.get_instance("origin")
        cursor = con.cursor()
    except Exception as e:
        logger.error(f"Error connecting to database: {e}", exc_info=True)
        return JSONResponse(content={"response": "Failed to connect to database."}, status_code=500)

    try:
        cursor.execute("""
                        select 
                            *
                        from
                            customer_attributes
                    """)

        result = cursor.fetchall() 
        df_customer_attributes = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description]) 

        cursor.execute("""
                        select 
                            *
                        from
                            customer_values
                    """)

        result = cursor.fetchall()
        df_customer_values = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description]) 

        cursor.execute("""
                        select 
                            *
                        from
                            user_attribute_categories
                    """)

        result = cursor.fetchall()
        df_user_attribute_categories = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description]) 

        cursor.execute("""
                        select 
                            *
                        from
                            user_attributes
                    """)

        result = cursor.fetchall()
        df_user_attributes = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description]) 

        cursor.close()

        # Get the attribute category name
        merged_df = pd.merge(df_user_attributes[['attribute_category_id', 'attribute_id']], df_user_attribute_categories[['id', 'name']], left_on='attribute_category_id', right_on='id', how='left')
        merged_df = merged_df[['attribute_id', 'name', 'attribute_category_id']].rename(columns={'name': 'attribute_category_name'})
        merged_df = pd.merge(df_customer_attributes[['id', 'name']], merged_df, left_on='id', right_on='attribute_id', how='left')
        merged_df = merged_df[[ 'name', 'attribute_id', 'attribute_category_name', 'attribute_category_id']].rename(columns={'name': 'attribute_name'})
        merged_df = pd.merge(df_customer_values[['attribute_id', 'value_text']], merged_df, on='attribute_id', how='left')
        merged_df = merged_df[['attribute_name', 'value_text', 'attribute_category_name']]
        merged_df = merged_df[(merged_df.attribute_category_name.notnull())&(merged_df.value_text != '')&(merged_df.value_text.notnull())]

        # Grouping and transforming into the desired format
        result_dict = (
            merged_df
            .groupby(['attribute_name','attribute_category_name'])['value_text']
            .apply(list)  # Combine 'value_text' into a list for each group
            .unstack()  # Pivot the table to have 'attribute_name' as keys
            .to_dict()  # Convert the DataFrame to a nested dictionary
        )

        # Cleaning up the nested dictionary structure
        dict_ = {
            category: {
                attr: list(set(values)) for attr, values in attributes.items() if isinstance(values, list)
            }
            for category, attributes in result_dict.items()
        }

        filtered_data = {
            category: {
                key: value
                for key, value in attributes.items()
                if key not in ["email", "phone", "name", "手機", "名稱", "生日","電話", "最近一次使用日期"]
            }
            for category, attributes in dict_.items()
        }

        final_dict = {}
        attributes = []
        sub_attributes = []
        for i in list(filtered_data.keys()):
            attributes.extend(filtered_data[i].keys())
            sub_attributes.extend(filtered_data[i].values())
            
        final_dict['data'] = filtered_data
        # final_dict['explanation'] = {
        #     "categoies": list(filtered_data.keys()),
        #     "attributes": attributes,
        #     "sub_attributes": sub_attributes
        # }

        print('final_dict:\n',final_dict)

        file_path = os.path.join("../app/data", "filtered_attributes.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(final_dict, f, ensure_ascii=False, indent=4)

        Database.close("origin")

        return JSONResponse(content={"response": "Filtered attributes saved."}, status_code=200)

    except Exception as e:
        logger.error(f"Error fetching data: {e}", exc_info=True)
        return JSONResponse(content={"response": "Failed to fetch data."}, status_code=500)


class AttributeRelevance(dspy.Signature):
    """Evaluate the relevance of a specific attribute to the given customer segment."""
    segment_name = dspy.InputField(desc="The name of the customer segment.")
    attribute_name = dspy.InputField(desc="The name of the attribute being assessed.")
    attribute_values = dspy.InputField(desc="The values associated with this attribute.")
    relevance_score = dspy.OutputField(desc="A score from 000.0 to 100.0 indicating relevance.")

def score_and_filter_attributes(json_attribute: dict):
    lm = global_vars.lm
    dspy.settings.configure(lm=lm, response_format=None)

    segment_name, attributes = json_attribute['segment'], json_attribute['selected_attributes']
    
    evaluate_relevance = dspy.ChainOfThought(AttributeRelevance)  # Uses step-by-step reasoning

    # Counting scores for recommended attributes
    count = 0
    scores = 0.0
    for category, attr_dict in attributes.items():
        for attr_name, attr_values in attr_dict.items():
            values_str = ", ".join(attr_values)

            try:
                # Query the LLM for relevance score
                response = evaluate_relevance(
                    segment_name=segment_name,
                    attribute_name=attr_name,
                    attribute_values=values_str
                )
            except Exception as e:
                logger.error(f"Error in LLM request: {e}", exc_info=True)
                return 0.0  # Return 0.0 if LLM request fails
            
            try:
                # Extract score safely
                score = float(response.relevance_score) if hasattr(response, 'relevance_score') and response.relevance_score else 0.0
                scores += score
                count += 1 
            except Exception as e:
                print(f"Error: {e}")
                continue

            print(f"Evaluated {attr_name}: Score {score}")
    avg_score = scores / count if count > 0 else 0.0
    
    return avg_score

def rule_out_exact_attributes(ai_response, single_attr):
    if ai_response['selected_attributes'] == single_attr:
        return {}
    else:
        return ai_response
    
def validate_attribute(attr_name, values, valid_attributes):

    if not valid_attributes or not values:
        return {
        }
    
    if attr_name not in valid_attributes:
        return {
        }
    
    return {
        attr_name: [i for i in values if i in valid_attributes.get(attr_name, [])]
    }

def validate_attributes(ai_response, valid_attributes):
    """Parallel attribute validation."""

    try:
        filtered_response = {}
        with ThreadPoolExecutor() as executor:
            future_to_attr = {
                executor.submit(validate_attribute, attr, values, valid_attributes.get(category, {})): (category, attr)
                for category, attributes in ai_response['selected_attributes'].items()
                for attr, values in attributes.items()
            }
            for future in future_to_attr:
                category, _ = future_to_attr[future]
                result = future.result()
                if category not in valid_attributes:
                    continue
                if category not in filtered_response:
                    filtered_response[category] = {}
                filtered_response[category].update(result)

        ai_response['selected_attributes'] = filtered_response
    except Exception as e:
        logger.error(f"Error in attribute validation: {e}", exc_info=True)
        return {
            'segment': '',
            'selected_attributes': {}
        }
    return ai_response


@router.get("/ai_recommendations_first", response_class=JSONResponse)
async def attribute_recommendation(
    user_id: str,
    user_name: str,
    user_input: Optional[str] = Query(..., max_length=20, title="Segment Name", description="Enter the segment name to generate attribute recommendations."),
    ):

    input_embedding = generate_embedding(user_input)
    logger.info(f"Embedding stored. Searching for similar segments...")
    
    conn = Database.get_instance("ai")
    if not conn:
        logger.error("Failed to connect to database.")
        return JSONResponse(content={"response": "Failed to connect to database."}, status_code=500)
    
    answers = search_similar_segments(conn, input_embedding)
    if answers:
        similar_segment = [
            {'recommendation_id': i[0],
            'recommendation_json': json.loads(i[1])
            } for i in answers
        ]
        print(f"Found similar segment answer: {similar_segment}")
        return JSONResponse(content={"similar_segment": similar_segment}, status_code=200)

    return await ai_rec_process(user_id, user_name, user_input, 3, input_embedding, conn)


@router.get('/ai_recommendations')
async def ai_rec_process(user_id:str = Query(..., title="User ID", description="User ID"),
                         user_name:str = Query(..., title="User Name", description="User Name"), 
                         user_input:str = Query(..., max_length=20, title="Segment Name", description="Enter the segment name to generate attribute recommendations."), 
                         counts:int = Query(..., title="recommendation_remain_Count", description="Number of recommendations to generate."),
                         input_embedding=None, 
                         conn=None):
    try:
        if input_embedding is None:
            input_embedding = generate_embedding(user_input)
            logger.info(f"Embedding stored. Searching for similar segments...")

        if conn is None:    
            conn = Database.get_instance("ai")
            if not conn:
                logger.error("Failed to connect to database.")
                return JSONResponse(content={"response": "Failed to connect to database."
                ""}, status_code=500)
            
        file_name = "filtered_attributes_fashion.json"
        file_path = os.path.join("../app/data", file_name)
        if not os.path.exists(file_path):
            print(f"File {file_name} not found.")
            return
        
        file_content = preprocess_json(file_path)

        retry = 0
        while retry < 4:
            logger.info(f"Generating attribute recommendations for segment: {user_input}")
            recommendation_ids = []
            final_dicts = []
            for count in range(counts):
                whole_attr = {}
                whole_category = {}
                final_dict = {}
                for category, att in file_content['data'].items():
                    for k, v in att.items():
                        
                        if client_stop_flags.get(user_id, False):
                            logger.warning(f"LLM generation stopped early by client: {user_id}")
                            client_stop_flags[user_id] = False
                            return JSONResponse(content={"response": "Stop signal received."}, status_code=200)
                        single_attr = {}

                        if k in ['已凍結','黑名單']:
                            continue

                        single_attr[category] = {k: v}
                        print('single_attr====================',single_attr)
                        answer = await asyncio.to_thread(generate_recommendations, user_input, single_attr, category, user_id)   
                        logger.debug(f'answer_after_generate_recommendations===================={answer}')
                        
                        if not answer or not answer['selected_attributes'].get(category) or isinstance(answer, str):
                            retry += 1
                            continue

                        answer = validate_attributes(answer, single_attr)
                        logger.debug(f'answer_after_validate_attributes===================={answer}')

                        if not answer or not answer['selected_attributes'].get(category):
                            retry += 1
                            continue
                        
                        answer = rule_out_exact_attributes(answer, single_attr)
                        logger.debug(f'answer_after_rule_out_exact_attributes===================={answer}')
                        if not answer or not answer['selected_attributes'].get(category):
                            continue
                        
                        answer.update({"group_id":count+1})
                        
                        for client in connected_clients:
                            try:
                                await client.send_json({"response": answer})
                            except Exception as e:
                                logger.error(f"Error sending message to client: {e}", exc_info=True)
                                continue

                        whole_attr.update(answer['selected_attributes'][category])
                    whole_category.update({category: whole_attr})
                    whole_attr = {}

                final_dict['selected_attributes'] = whole_category
                final_dict['segment'] = user_input

                rec_explanation = await asyncio.to_thread(generate_explanation, final_dict)
                logger.debug(f'rec_explanation===================={rec_explanation}')
                if rec_explanation:
                    for client in connected_clients:
                        try:
                            await client.send_json({"segment": user_input, "response": rec_explanation})
                        except Exception as e:
                            logger.error(f"Error sending message to client: {e}", exc_info=True)
                            continue
                if rec_explanation:
                    rec_explanation = json.loads(rec_explanation)
                else:
                    rec_explanation = {}
                final_dict.update(rec_explanation)
                final_dicts.append(final_dict)
                logger.debug(f'final_dict===================={final_dict}')
            try:
                for final_dict in final_dicts:
                    score = score_and_filter_attributes(final_dict)
                    logger.info(f"Segment name [{user_input}] matches input.")

                    try:
                        # Convert the dictionary to a JSON string in order to store it in the database
                        answer = json.dumps(final_dict, ensure_ascii=False)
                        rec_embedding = generate_embedding(answer)
                        recommendation_ids.append(store_embedding(conn, user_id, user_name, user_input, answer, input_embedding, rec_embedding, score))
                        # Convert JSON to dictionary
                    except json.JSONDecodeError as e:
                        logger.error("Error loading JSON: {e}")
                        continue

            except Exception as e:
                logger.error(f"Error loading JSON: {e}", exc_info=True)

            return JSONResponse(content={"recommendation_id": recommendation_ids}, status_code=200)   
            
        else:
            logger.error("Failed to generate attribute recommendations.", exc_info=True
            )
            return JSONResponse(content={"response": "Failed to generate attribute recommendations."}, status_code=500)
    except Exception as e:
        logger.error(f"Error in AI recommendation process: {e}", exc_info=True)
        return JSONResponse(content={"response": "Failed to generate attribute recommendations."}, status_code=500)
    finally:
        Database.close("ai")
        client_stop_flags.pop(user_id, None)


@router.post("/ai_recommendations_user",description= """
            user_modified_json example below: \n
            {
             "segment": "szdv", 
            "selected_attributes": {
                "消費屬性": {
                    "每月平均消費": ["12000", "11500", "5500", "7200", "10000", "9000", "8500", "6000", "8000", "6800"]
                    }
                }
            }
                                                    """)
def store_user_recommendation(recommendation_id: int, 
                              user_modified_json: str =  Query(..., description='It must include segment and selected_attributes')):
    try:
        conn = Database.get_instance("ai")
        cur = conn.cursor()

        user_embedding = generate_embedding(user_modified_json)
        dict_json = json.loads(user_modified_json)
        print('user_modified_json--------------:', dict_json)
        score = score_and_filter_attributes(dict_json)
        # Create hash ID from content
        hash_id = hashlib.sha256(user_modified_json.encode()).hexdigest()

        try:
            cur.execute("""
                INSERT INTO user_recommendations (
                    id, recommendation_id, recommend_json_user, embedding_recommend_user, eval_score, accepted
                )
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (hash_id, recommendation_id, user_modified_json, user_embedding, score, True))

            conn.commit()
            Database.close("ai")
            return JSONResponse(content={"response": "User recommendation stored successfully."}, status_code=200)
        except psycopg2.errors.UniqueViolation:
            logger.error(f"Error inserting user recommendation", exc_info=True)
            conn.rollback()
            return JSONResponse(content={"response": "Duplicate entry."}, status_code=200)

    except Exception as e:
        logger.error(f"Error storing user recommendation: {e}", exc_info=True)
        return JSONResponse(content={"response": "Failed to store user recommendation."}, status_code=500)


@router.get("/ai_warm_up")
def ai_warm_up():
    """Warm up the AI model by sending a test message."""
    client = global_vars.client

    try:
        response = client.chat(
            model=model_name,
            messages=[{"role": "user", "content": "你在嗎？"}]
        )
        print("Model warmed up:", response["message"]["content"])
        return JSONResponse(content={"response": "Model warmed up."}, status_code=200)
    except Exception as e:
        logger.error(f"Error warming up model: {e}", exc_info=True)
        return JSONResponse(content={"response": "Failed to warm up model."}, status_code=500)


connected_clients = []
# Have frontend to connect to ws
# Example => ws://localhost:8000/seg_ai/ws/attribute_updates?user_id=Mark123 
@router.websocket("/ws/attribute_updates")
async def websocket_endpoint(websocket: WebSocket, user_id: str = Query(...)):
    """Websocket endpoint for attribute updates."""
    await websocket.accept()
    print('websocket connected', websocket)
    connected_clients.append(websocket)

    client_id = user_id

    client_stop_flags[client_id] = False

    try:
        while True:
            msg = await websocket.receive_text() # Keep connection alive
            if msg.strip() == 'stop':
                client_stop_flags[client_id] = True
                await websocket.send_text("Stop signal received. Closing connection.")
                if websocket in connected_clients:
                    connected_clients.remove(websocket)
                    logger.info(f"Client {client_id} disconnected.")
                await websocket.close()
                break
    except WebSocketDisconnect:
        connected_clients.remove(websocket)


if __name__ == "__main__":
    attribute_recommendation(1, 'test', '高端客戶')

# ws://127.0.0.1:8000/seg_ai/ws/attribute_updates
# ws://192.168.2.30:8000/seg_ai/ws/attribute_updates