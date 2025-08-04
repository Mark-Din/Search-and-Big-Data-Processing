import pandas as pd
import numpy as np
import re
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA, NMF
from sklearn.cluster import KMeans
from connection import ElasticSearchConnectionManager
from TCSP import read_stopwords_list

import warnings
import logging
import spacy
from common import initlog
warnings.filterwarnings('ignore')

# nltk.download('punkt')
# nltk.download('stopwords')
# nltk.download('wordnet')
# nlp = spacy.load('zh_core_web_md')

import pandas as pd
import mysql.connector

# Initialize logger
logger = initlog('common')


def extract_data(tables):
    
    # Initialize DataFrames
    user_df = pd.DataFrame()
    course_df = pd.DataFrame()
    lifecircle_df = pd.DataFrame()
    
    # Establish MySQL connection
    conn = ElasticSearchConnectionManager.mysql_connection_nexva()

    try:
        # Loop through tables
        for table in tables:
            # Check if connection is still active
            if conn.is_connected():
                print(f'[{tables}] connection established')
            else:
                print('Connection lost')
                conn = ElasticSearchConnectionManager.mysql_connection_nexva()

            # Create cursor and execute query
            cursor = conn.cursor(dictionary=True)
            cursor.execute(f'SELECT * FROM {table}')
            data = cursor.fetchall()

            # Assign data to respective DataFrames
            if 'user' in table:
                user_df = user_df.append(data, ignore_index=True)
            elif 'article' in table:
                course_df = course_df.append(data, ignore_index=True)
            else:
                lifecircle_df = lifecircle_df.append(data, ignore_index=True)
    except mysql.connector.Error as e:
        print(f'Error connecting to MySQL: {e}')
    finally:
        # Close connection
        if conn.is_connected():
            conn.close()
            print('MySQL connection closed')
            
    return user_df, course_df, lifecircle_df
    

def process_data_for_ml(user_df=pd.DataFrame(), course_df=pd.DataFrame(), lifecircle_df=pd.DataFrame()):

    '''Process data for machine learning.'''
    course_df_for_training = course_df[course_df['disabled'] == 0]
    # lifecircle_df = lifecircle_df[lifecircle_df['deleted'] == 0]

    '''At this moment of time, only train course_df and lifecircle_df'''
    # table: [userESG] userType 0: 學生, 1: 講師(個人), 2: 企業 3: 講師(單位)
    course_df_for_training = course_df_for_training[['price', 'content', 'createdAt']]
    # lifecircle_df_for_training = lifecircle_df[['visibility', 'about', 'createdAt']]
    # Get the information about instructors
    # user_df_for_training = user_df[user_df['userType'] == '3'][[ 'experienceESG', 'experience', 'education', 'personalIntroduce', 'companyName', 'jobTitle', 'createdAt']]
    # Combine ['experienceESG','experience','education'] into one
        
    course_df_for_training['content'] = course_df_for_training['content'].apply(lambda x : re.sub(r'<[^>]*>|&nbsp;','', x ))    

    return course_df_for_training, course_df


# Data Preparation
def preprocess_data(df):
    """Prepare data by encoding, scaling and extracting datetime features."""
    
    scaler = StandardScaler()
    df[['price']] = scaler.fit_transform(df[['price']])
    
    df['createdAt'] = pd.to_datetime(df['createdAt'])
    df['year'] = df['createdAt'].dt.year
    df['month'] = df['createdAt'].dt.month
    df['day'] = df['createdAt'].dt.day
    return df, scaler

# Function to clean text
def clean_text(text):
    # Remove text within parentheses and the parentheses themselves
    text = re.sub(r'[\[\(\)\]]', '', text)
    text = text.strip()  # Remove leading/trailing whitespace
    return text

def process_text(df):
    """Process text data by tokenizing, removing stopwords, and lemmatizing."""

    nlp = spacy.load('zh_core_web_md')

    df['content_clean'] = df['content'].apply(clean_text)
    df['content_clean'] = df['content_clean'].apply(lambda x: re.sub(r'<[^>]*>|&nbsp;', '', x))
    df['content_tokenized'] = df['content_clean'].apply(lambda x: [token.text for token in nlp(x)])
    
    stop_words = set(stopwords.words('chinese'))
    # Assuming read_stopwords_list is a function you've defined elsewhere
    stop_words.update(read_stopwords_list())
    df['content_tokenized'] = df['content_tokenized'].apply(lambda x: [word for word in x if word not in stop_words])
    
    # Lemmatizing for English text
    lemmatizer = WordNetLemmatizer()
    df['content_lemmatized'] = df['content_tokenized'].apply(lambda x: [lemmatizer.lemmatize(word) for word in x])
    df['content_processed'] = df['content_lemmatized'].apply(' '.join)

    # Convert and clean final data
    df = df.applymap(convert_element)

    return df


# Modeling
def vectorize_and_model(df, scaler):
    """Vectorize text and apply NMF and other models."""
    vectorizer = TfidfVectorizer(max_features=1000)
    X_text = vectorizer.fit_transform(df['content_processed'])
    
    nmf_model = NMF(n_components=20, random_state=42)
    nmf_model.fit(X_text)
    topic_results = nmf_model.transform(X_text)
    
    pca = PCA(n_components=19)
    features = df[['price', 'year', 'month', 'day',]].values
    features = np.hstack((features, topic_results))
    features_pca = pca.fit_transform(features)
    
    kmeans = KMeans(n_clusters=19, random_state=42)
    kmeans.fit(features_pca)
    df['cluster'] = kmeans.labels_
    
    scaled_df_es = scaler.fit_transform(df[['cluster']])

    df_encoded = pd.concat([
        pd.DataFrame(scaled_df_es),
        pd.DataFrame(features)
    ], axis=1)

    df_encoded.index = df.index.to_list()

    return df, vectorizer, nmf_model, pca, kmeans, df_encoded


# Utility Functions
def convert_element(x):
    """Convert elements for final DataFrame."""
    if isinstance(x, bytearray):
        return x.decode()
    elif isinstance(x, pd.Timestamp):
        return x.isoformat()
    return x


# def main():

#     tables = ['userESG', 'lifeCircleESG', 'courseESG']
#     user_df, course_df, lifecircle_df = extract_data(tables)
    
#     # Process data, tokenize, and vectorize
#     course_df_for_training, course_df = process_data_for_ml(user_df, course_df, lifecircle_df)

#     # Load and prepare data
#     data, scaler = preprocess_data(course_df_for_training.copy(deep=True))

#     # Process text
#     data = process_text(data)
    
#     # Modeling
#     data, vectorizer, nmf_model, pca, kmeans, df_encoded = vectorize_and_model(data, scaler)
    
#     # Save or further process your `course_df` as needed
#     final_df = course_df.copy(deep=True)

#     final_df['attributes_vector'] = df_encoded.apply(lambda row: row.tolist(), axis=1)
#     final_df = final_df.where(pd.notnull(final_df), None)
    
#     actions = es_import_main(final_df, df_encoded)
    
#     logger.info(f"Data indexed successfully for {tables} with {len(actions)} records")


# # Main Execution
# if __name__ == "__main__":

#     main()
