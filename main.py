from dotenv import load_dotenv
import os
import google.generativeai as genai
import psycopg2
import re
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# load all environment variables
load_dotenv()

# configure API key
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

# load google gemini model
model=genai.GenerativeModel("gemini-1.5-pro-latest")

prompt=[
    """
        You are an expert SQL assistant. Your task is to generate SQL queries on postgreSQL database based on natural language requests. The database name is defaultdb , which contains the following table:
        daily_ads_data (brand_id int4, brand_name text, channel text, account_id int4, channel_campaign_id int4, campaign_name text, channel_ad_group_id int4, ad_group_name text, channel_ad_id int4, channel_asset_id int4, asset_type text, asset_source text, asset_height int4, asset_width int4, asset_orientation text, insights_date text, spend float4, clicks int4, impressions int4, channel_metrics text, gender text, offer_in_ad text, background_color text, audio_present text, person_status text, media_entities text, media_text_overlay text, ctr float4, cpc float4, cpm float4, cpa float4, cpp float4, roas float4, Cost/Registration float4, conversions float4, purchases float4, Purchase Value float4, atcs float4, leads float4, registrations float4, Page Likes float4)
        
        Important to note - 
        1. Always assume that text fields may have inconsistent casing (eg., Google, google, GOOGLE).
        2. For comparisons in where clauses on text columns like brand_name, channel, gender, asset_type, etc. always use LOWER(column_name) = 'value_in_lowercase'. also, make sure the right-hand-side value (in quotes) is fully lowercase. Do not use Title Case or UPPERCASE.

        For example, 
        Example 1 - How many unique entries of brand are there ?, the sql command will be something like this select count(distinct brand_id) from daily_ads_data; 
        Example 2 - Tell me mostly used shape of ads, the sql command will be something like select asset_type, count(*) AS count from daily_ads_data group by asset_type order by count desc limit 1; 
        Example 3 - give me count of total impressions made per brand using google in decreasing order ? sql query will be select brand_name, sum(impressions) as total_impressions from daily_ads_data where lower(channel)='google' group by total_impressions desc;
        Example 4 - tell me about brands having most and least impressions ? sql query will be like with max_impression as (select brand_name from daily_ads_data order by impressions DESC limit 1), min_impression as (select brand_name from daily_ads_data order by impressions asc limit 1) select * from max_impression union select * from min_impression;
        Example 5 - what's total spend per day in last 7 days? sql query will be like select insights_date, sum(spend) as total_spend from daily_ads_data where insights_date >= current_date - interval '7 days' group by insights_date;
        
        also the sql code should not have ```sql in beginning or end and sql word in output.
    """,
    
    """
        You are an AI assistant that converts SQL query results into natural language explanations.
        
        Given the following user question, corresponding SQL query, and SQL result, answer the user question with proper sentence building and spacing between words.
        Question:  {question}
        SQL Query: {sql_query}
        SQL Result: {sql_result}
    """
]

'''
# Enlist all available models
models = genai.list_models()
for model in models:
    print(model.name)
'''
    
# connect database
def connect_database():
    
    try:
        conn = psycopg2.connect(dbname = os.getenv("DB_NAME"), 
                                user = os.getenv("DB_USER"), 
                                password = os.getenv("DB_PASSWORD"),
                                host = os.getenv("DB_HOST"), 
                                port = os.getenv("DB_PORT"))
        return conn
    
    except:
        raise HTTPException(status_code = 500, detail = "Database connection failed.")
   
# generate sql query
def get_sql_query(question: str, prompt):
    response=model.generate_content([prompt[0], question])
    
    if "select" in response.text.lower():
        sql = response.text.strip()
        sql = re.sub(r"^```sql\s*|\s*```$", "", sql).strip()
        return sql
    else:
        raise HTTPException(status_code = 400, detail = "Illegal SQL query.")
        
# execute sql query
def execute_sql_query(sql : str):
    try:
        conn = connect_database()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()        
        cur.close()
        conn.close()
          
        return rows
    
    except:
        raise HTTPException(status_code=500, detail="Error: Failed to execute SQL query.")

# generate answer
def generate_answer(sql_query: str, sql_result: list,question : str, prompt_template):
    if not sql_result:
        return "No results found"
    
    formatted_result = "\n".join([str(row) for row in sql_result])
    
    prompt=prompt_template[1].format(sql_query=sql_query, sql_result=formatted_result, question=question)
    
    response=model.generate_content(prompt)
    
    return response.text.strip() if response else "Couldnt generate an outcome."
  
# Initialize FastAPI
app = FastAPI()

# Pydantic model for handling incoming question
class Question(BaseModel):
    question: str
 
 # root endpoint   
@app.get("/")
def root():
    return {"message" : "Welcome to chat assistance"}

# endpoint to handle questions from user
@app.post("/question")
def question_handler(ques: Question):
    question=ques.question
    try:
        sql_query = get_sql_query(question, prompt)
        sql_result = execute_sql_query(sql_query)
        answer = generate_answer(sql_query, sql_result, question, prompt)
        
        return {
            "question":question,
            "sql_query":sql_query,
            "sql_result":sql_result,
            "answer": answer
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



