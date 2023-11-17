import datetime
import logging
import airflow
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import gcsfs

today_date = f'{datetime.datetime.now().year}_{datetime.datetime.now().month}_{datetime.datetime.now().day}'

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define Google Cloud Storage bucket
gcs_bucket = 'us-central1-testinggcpairfl-b6522ecc-bucket'
fs = gcsfs.GCSFileSystem(project="My First Project")

# Define constants
companies = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]

# Define tasks
def start_dag():
    logger.info("Starting Transformation process")

def end_dag():
    logger.info("Transformation process ended")

def calculate_cagr(most_recent_close, furthest_recent_close, years):
    return ((most_recent_close / furthest_recent_close) ** (1 / years)) - 1

def read_csv_from_gcs(file_path):
    # Read CSV file from GCS
    fs = gcsfs.GCSFileSystem(project="My First Project")
    with fs.open(file_path, "r") as f:
        df = pd.read_csv(f)
    return df

def get_cagr():
    logger.info("Get CAGR started")
    
    try:
        share_prices_data = read_csv_from_gcs(f"{gcs_bucket}/data/clean_data/share_prices.csv")
        
        share_prices = pd.DataFrame(data=share_prices_data)
        
        share_prices['Date'] = pd.to_datetime(share_prices['Date'])
        overall_cagr = []
        
        for company in companies:
            company_cagr = [company]
            df = share_prices[share_prices['symbol'] == company].sort_values(by='Date')
            
            most_recent_close = df.iloc[-1]['Close']
            most_recent_year = df.iloc[-1]['Date'].year
    
            furthest_recent_close = df.iloc[0]['Close']
            furthest_recent_year = df.iloc[0]['Date'].year
    
            years = most_recent_year - furthest_recent_year
    
            company_cagr.append(years)
            cagr = calculate_cagr(most_recent_close, furthest_recent_close, years)
            company_cagr.append(cagr)
            overall_cagr.append(company_cagr)
    
        df = pd.DataFrame(data=overall_cagr, columns=['symbol', 'years', 'cagr'])
        
        csv_data = df.to_csv(index=False)
        
        with fs.open(f"{gcs_bucket}/data/clean_data/cagr.csv", "w") as f:
            f.write(csv_data)
        
        logger.info("Get CAGR ended")
    except Exception as e:
        logger.error(f"An error occurred in get_cagr: {str(e)}")

def get_pe():
    logger.info("Get PE started")
    
    try:
        eps_data_point = read_csv_from_gcs(f"{gcs_bucket}/data/raw_data/eps.csv")
        share_prices_data = read_csv_from_gcs(f"{gcs_bucket}/data/clean_data/share_prices.csv")
        
        share_prices = pd.DataFrame(data=share_prices_data)
        eps_data = pd.DataFrame(data=eps_data_point)
        
        share_prices['Date'] = pd.to_datetime(share_prices['Date'])
        
        flattened_items = []
        
        for company in companies:
            df = share_prices[share_prices['symbol'] == company].sort_values(by='Date')

            price = df.iloc[-1]['Close']

            eps = eps_data[eps_data['symbol'] == company]['eps'].values[0]
                
            pe = price/eps
            
            flattened_item = {
                'symbol': company,
                'pe': pe
            }
    
            flattened_items.append(flattened_item)
    
        df = pd.DataFrame(data=flattened_items, columns=['symbol', 'pe'])
        
        csv_data = df.to_csv(index=False)
        
        with fs.open(f"{gcs_bucket}/data/clean_data/pe.csv", "w") as f:
            f.write(csv_data)
        
        logger.info("Get PE ended")
    except Exception as e:
        logger.error(f"An error occurred in get_pe: {str(e)}")

def insert_sentiment():
    logger.info("Insert into sentiment started")
    
    sentiment_data = read_csv_from_gcs(f"{gcs_bucket}/data/raw_data/sentiment_{today_date}.csv")
    
    with fs.open(f"{gcs_bucket}/data/clean_data/sentiment.csv", "a") as f:
        f.write(sentiment_data.to_csv(index=False))
    
    logger.info("Insert into sentiment ended")
    
def insert_share_price():
    logger.info("Insert into share_prices started")
    
    sentiment_data = read_csv_from_gcs(f"{gcs_bucket}/data/raw_data/share_price_{today_date}.csv")
    
    with fs.open(f"{gcs_bucket}/data/clean_data/share_prices.csv", "a") as f:
        f.write(sentiment_data.to_csv(index=False))
    
    logger.info("Insert into share_prices ended")

# Define the Airflow DAG and its configurations
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

# Create the DAG instance
transformation_dag = DAG(
    'transformation_dag',
    default_args=default_args, 
    description='Transform data from GCS to be used in BigQuery',
    schedule_interval="0 22 * * 1-5",
    catchup=False
)

# Define DAG tasks
start_dag = PythonOperator(
    task_id='start_dag',
    python_callable=start_dag,
    dag=transformation_dag
) 

end_dag = PythonOperator(
    task_id='end_dag',
    python_callable=end_dag,
    dag=transformation_dag
) 

get_cagr = PythonOperator(
    task_id='get_cagr',
    python_callable=get_cagr,
    dag=transformation_dag
) 

insert_sentiment = PythonOperator(
    task_id='insert_sentiment',
    python_callable=insert_sentiment,
    dag=transformation_dag
) 

insert_share_price = PythonOperator(
    task_id='insert_share_price',
    python_callable=insert_share_price,
    dag=transformation_dag
) 

get_pe = PythonOperator(
    task_id='get_pe',
    python_callable=get_pe,
    dag=transformation_dag
) 

start_dag >> insert_sentiment >> insert_share_price >> get_cagr >> get_pe >> end_dag