# Search system implemented in Python
## Description
This is a simple search system implemented in Python. It uses the inverted index data structure to store the words and their corresponding documents. The system can search for documents that contain a specific word or a combination of words. The search results are ranked based on the number of occurrences of the search terms in the documents.

## Deployment
### Requirements
- Python 3.8
- Elasticsearch 7.10.2
- MySQL 8.0.23
- MongoDB 4.4.4

## Evirontment
- Ubuntu 20.04 => docker

## Cron job
### ETL for transfering data from MySQL to Elasticsearch
*/5 * * * * /usr/bin/python3.8 /opt/allgets/Python_ETL/ETL_mysql_mysql_elasticsearch >> /opt/esg/Python_ETL/ETL_mysql_mysql_elasticsearch/data_update_log.log 2>&1

### Run keyword_processing for processing keyword
0 4,16 * * * /usr/bin/python3.8 /opt/allgets/Python_ETL/ETL_ESG_mongodb_mysqlkeyword_pyscript.py >> /opt/esg/Python_ETL/ETL_ESG_mongodb_mysql/keyword_logfile.log 2>&1

=============================================================================================================================================================
# ETL in Python
## QuickReport URL
http://172.105.198.49:9000<br>
account: nexva<br>
password: nexva


## 資料庫架構
MongoDB: Nodebb (用 "_key" 來篩選需要的資料)<br>
MySQL: Nexva 主要使用其 log 表格<br>
MySQL: quickreport 將所有分析資料放在此處<br>


## 關鍵字處理
### keyword script.py
  
  將會優化成以下流程
  ```
  1. 用 jieba 將關鍵字切成詞 
  2. 用 stopwords 去除停用詞
  3. 用 TF-IDF 找出關鍵字
  4. 用 wordcloud 畫出文字雲
  ```

## Process
1. Change the timezone in Linux to Asia/Taipei
2. Create venv and install the required packages
3. Run the script to test in the venv
4. Create a cron job to run the script every day at 4 and 16