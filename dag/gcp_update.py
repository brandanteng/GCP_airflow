import datetime
import logging
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set project and table ID
project_id = 'prefab-list-404501'
table_id = 'gcp_airflow.cagr'

# Initialize BigQuery client
client = bigquery.Client()

# Define Google Cloud Storage bucket
gcs_bucket = 'us-central1-testinggcpairfl-b6522ecc-bucket'

# Set job configuration
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True, 
)

# Define tasks
def start_dag():
    logger.info("Starting Transformation process")

def end_dag():
    logger.info("Transformation process ended")

def load_cagr():
    cagr = 'data/clean_data/cagr.csv'
    cagr_uri = f'gs://{gcs_bucket}/{cagr}'
    
    job = client.load_table_from_uri(cagr_uri, project_id + '.' + table_id, job_config=job_config)
    job.result()
    
def load_financial_profile():
    financial_profile = 'data/raw_data/financial_profile.csv'
    financial_profile_uri = f'gs://{gcs_bucket}/{financial_profile}'
    
    job = client.load_table_from_uri(financial_profile_uri, project_id + '.' + table_id, job_config=job_config)
    job.result()

def load_growth_rates():
    growth_rates = 'data/raw_data/growth_rates.csv'
    growth_rates_uri = f'gs://{gcs_bucket}/{growth_rates}'
    
    job = client.load_table_from_uri(growth_rates_uri, project_id + '.' + table_id, job_config=job_config)
    job.result()


def load_income_statement():
    income_statement = 'data/raw_data/income_statement.csv'
    income_statement_uri = f'gs://{gcs_bucket}/{income_statement}'
    
    job = client.load_table_from_uri(income_statement_uri, project_id + '.' + table_id, job_config=job_config)
    job.result()

def load_profile():
    profile = 'data/raw_data/profile.csv'
    profile_uri = f'gs://{gcs_bucket}/{profile}'
    
    job = client.load_table_from_uri(profile_uri, project_id + '.' + table_id, job_config=job_config)
    job.result()

def load_sentiment():
    sentiment = 'data/clean_data/sentiment.csv'
    sentiment_uri = f'gs://{gcs_bucket}/{sentiment}'
    
    job = client.load_table_from_uri(sentiment_uri, project_id + '.' + table_id, job_config=job_config)
    job.result()

def load_share_price():
    share_price = 'data/clean_data/share_prices.csv'
    share_price_uri = f'gs://{gcs_bucket}/{share_price}'
    
    job = client.load_table_from_uri(share_price_uri, project_id + '.' + table_id, job_config=job_config)
    job.result()
    
def load_pe():
    pe = 'data/clean_data/pe.csv'
    pe_uri = f'gs://{gcs_bucket}/{pe}'
    
    job = client.load_table_from_uri(pe_uri, project_id + '.' + table_id, job_config=job_config)
    job.result()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

update_dag = DAG(
    'update_dag',
    default_args=default_args, 
    description='Load data from GCS to BigQuery',
    schedule_interval= None,
    catchup=False
)

# Define DAG tasks
start_dag = PythonOperator(
    task_id='start_dag',
    python_callable=start_dag,
    dag=update_dag
) 

end_dag = PythonOperator(
    task_id='end_dag',
    python_callable=end_dag,
    dag=update_dag
) 

load_cagr = PythonOperator(
    task_id='load_cagr',
    python_callable=load_cagr,
    dag=update_dag
)

load_financial_profile = PythonOperator(
    task_id='load_financial_profile',
    python_callable=load_financial_profile,
    dag=update_dag
)

load_growth_rates = PythonOperator(
    task_id='load_growth_rates',
    python_callable=load_growth_rates,
    dag=update_dag
)

load_income_statement = PythonOperator(
    task_id='load_income_statement',
    python_callable=load_income_statement,
    dag=update_dag
)

load_profile = PythonOperator(
    task_id='load_profile',
    python_callable=load_profile,
    dag=update_dag
)

load_sentiment = PythonOperator(
    task_id='load_sentiment',
    python_callable=load_sentiment,
    dag=update_dag
)

load_share_price = PythonOperator(
    task_id='load_share_price',
    python_callable=load_share_price,
    dag=update_dag
)

load_pe = PythonOperator(
    task_id='load_pe',
    python_callable=load_pe,
    dag=update_dag
)

start_dag >> load_cagr >> load_financial_profile >> load_growth_rates >> load_income_statement >> load_profile >> load_sentiment >> load_share_price >> load_pe >> end_dag