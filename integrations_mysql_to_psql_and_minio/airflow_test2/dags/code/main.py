import pandas as pd

import uuid
from sqlalchemy import MetaData
from sqlalchemy import text

import uuid

import uuid
import logging

from query_insert import upsert_data, get_data, query

logger = logging.getLogger(__name__)


def uuid_generator(value) -> str:
    # Namespace UUID from your config
    ID_SEED = uuid.UUID("6bb2e11b-6442-4e3b-a823-95acc9a96380")

    # Example usage:
    return str(uuid.uuid5(ID_SEED, value))


def main():

    try:
        user_df = get_data()

        user_df.to_csv('./user_df.csv',index=False)
        
        attributes = []
        for attribute in user_df.columns:
            if attribute in ['id']:
                continue
            attributes.append({"id": uuid_generator(attribute), "name": attribute, "value_type": "text", "is_required": False, 
                            "is_ui": True, "is_editable": True, 'default_value_text': None, 'default_value_integer': None,
                            'default_value_float': None, 'default_value_boolean': None, 'default_value_date': None,
                            'default_value_varchar_list': None})

        upsert_data("customer_attributes", attributes, ["name"])

        # Extract id and name from customer_attributes
        id_ = []
        for uid in user_df.id:
            id_.append({'id': uid})

        upsert_data("customers", id_, ['id'])

        col_to_id = {row[0]:row[1] for row in query(user_df)}
        values = []
        for _, row in user_df.iterrows():
            for index, value in enumerate(row):
                if row.keys()[index] in col_to_id:
                    if value == row.id:
                        continue
                    customer_id = row.id
                    attribute_id = col_to_id[row.keys()[index]]
                    values.append({
                        'id':uuid_generator(str(customer_id) + str(attribute_id)), 
                        'customer_id': str(customer_id), 
                        'attribute_id': str(attribute_id),
                        'value_text': value
                        })
        upsert_data("customer_values", values, ['id'])

        # Create user_attribute_categories
        name = 'Nexva 屬性'
        attribute_category_id = uuid_generator(name)
        upsert_data("user_attribute_categories", [{
            'id': attribute_category_id,
            'name': name,
            'user_id': '31f4f622-54f1-493f-821a-7bf03790fdbd',
            'priority': 1
        }], ['id'])

        # Assign attribute to user_attributes
        user_attributes = []
        priority = 0
        for attribute in attributes:
            user_attributes.append({
                'id':uuid_generator(str(attribute['id'])+str(attribute_category_id)),
                'user_id':'31f4f622-54f1-493f-821a-7bf03790fdbd',
                'attribute_id':str(attribute['id']),
                'attribute_category_id':str(attribute_category_id),
                'priority':priority
                })

            priority +=1

        upsert_data("user_attributes", user_attributes, ['id'])
    
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise

if __name__ == '__main__':
    
  main()