import datetime
import logging
import airflow
import pandas as pd
import requests
import gcsfs


from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define Google Cloud Storage bucket and file path
gcs_bucket = 'us-central1-testinggcpairfl-b6522ecc-bucket'
fs = gcsfs.GCSFileSystem(project="My First Project")

#Define constants
companies = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]

today_date = f'{datetime.datetime.now().year}_{datetime.datetime.now().month}_{datetime.datetime.now().day}'

# Define extraction tasks
def start_dag():
    logger.info(f"Starting Extraction process")
    
def end_dag():
    logger.info(f"Extraction process ended")

def get_profile():
    url = "https://financialmodelingprep.com/api/v3/profile/"

    api_key = "4eiHO5ke53QNwBQEO3zSPVMaH5kEiPJq"

    headers = {
        "Content-Type": "application/json"
    }

    params = {
        "apikey": api_key
    }

    flattened_items = []

    logger.info(f"Starting to get profile")
    
    for company in companies:
        response = requests.get(url+company, headers=headers, params=params)

        if response.status_code == 200:
            data_things = response.json()
            flattened_item = {
                'symbol': data_things[0]['symbol'],
                'sector': data_things[0]['sector'],
                'industry': data_things[0]['industry'],
                'description': data_things[0]['description']
            }
            
            flattened_items.append(flattened_item)

        else:
            url_backup = "https://www.alphavantage.co/query?function=OVERVIEW&symbol="
            
            api_key_backup = "7EM86RAEWREII1BH"

            headers_backup = {
                "Content-Type": "application/json"
            }

            params_backup = {
                "apikey": api_key_backup
            }

            response_backup = requests.get(url_backup+company, headers=headers_backup, params=params_backup)
            
            if response_backup.status_code == 200:
                data_things = response_backup.json()
                flattened_item = {
                    'symbol': data_things[0]['Symbol'],
                    'sector': data_things[0]['Sector'],
                    'industry': data_things[0]['Industry'],
                    'description': data_things[0]['Description']
                }
            
            flattened_items.append(flattened_item)
            

    df = pd.DataFrame(flattened_items)
                    
    csv_data = df.to_csv(index=False)
                
    with fs.open(f"{gcs_bucket}/data/raw_data/profile.csv", "w") as f:
        f.write(csv_data)

    logger.info(f"Get profile ended")
    
def get_financial_profile():
    url = "https://www.alphavantage.co/query?function=CASH_FLOW&symbol="
        
    api_key = "7EM86RAEWREII1BH"

    headers = {
        "Content-Type": "application/json"
    }

    params = {
        "apikey": api_key
    }

    flattened_items = []

    logger.info(f"Starting to get financial profile")

    for company in companies:
        response = requests.get(url+company, headers=headers, params=params)
                
        if response.status_code == 200:
            data_things = response.json()
            for item in data_things['annualReports']:
                flattened_item = {
                    "symbol": company,
                    'fiscalDateEnding': item['fiscalDateEnding'],
                    'operatingCashflow': item['operatingCashflow'],
                    'cashflowFromInvestment': item['cashflowFromInvestment'],
                    'cashflowFromFinancing': item['cashflowFromFinancing'],
                    'netIncome': item['netIncome']
                }
                
                flattened_items.append(flattened_item)
        else:
            url_backup = "https://financialmodelingprep.com/api/v3/cash-flow-statement/AAPL"

            api_key_backup = "4eiHO5ke53QNwBQEO3zSPVMaH5kEiPJq"

            headers_backup = {
                "Content-Type": "application/json"
            }

            params_backup = {
                'period': 'annual',
                "apikey": api_key_backup
            }
            
            response_backup = requests.get(url_backup, headers=headers_backup, params=params_backup)

            if response_backup.status_code == 200:
                data_things = response_backup.json()
                for item in data_things[0]:
                    flattened_item = {
                        "symbol": company,
                        'fiscalDateEnding': item['date'],
                        'operatingCashflow': item['operatingCashFlow'],
                        'cashflowFromInvestment': item['netCashUsedForInvestingActivites'],
                        'cashflowFromFinancing': item['netCashProvidedByOperatingActivities'],
                        'netIncome': item['netIncome']
                    }
                    
                    flattened_items.append(flattened_item)

    df = pd.DataFrame(flattened_items)
                    
    csv_data = df.to_csv(index=False)
                
    with fs.open(f"{gcs_bucket}/data/raw_data/financial_profile.csv", "w") as f:
        f.write(csv_data)

    logger.info(f"Get financial profile ended")
    
    
def get_share_price():
    url = "https://financialmodelingprep.com/api/v3/quote/"
    
    api_key = "4eiHO5ke53QNwBQEO3zSPVMaH5kEiPJq"
    
    headers = { 
        "Content-Type": "application/json"
    } 

    params = {
        "apikey": api_key
    }

    flattened_items = []

    logger.info(f"Starting to get daily share price")
    
    for company in companies:
        response = requests.get(url+company, headers=headers, params=params)
                
        if response.status_code == 200:
            data_things = response.json()
            for item in data_things:
                flattened_item = {
                    "symbol": company,
                    '‘Date': f'{datetime.datetime.now().day}/{datetime.datetime.now().month}/{datetime.datetime.now().year}',
                    'Open': item['open'],
                    'High': item['dayHigh'],
                    'Low': item['dayLow'],
                    'Close': item['previousClose'],
                    'Price': item['price'],
                    'Volume': item['volume']
                }
                
                flattened_items.append(flattened_item)
        else:
            url_backup = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol="
            
            api_key_backup = "7EM86RAEWREII1BH"

            headers_backup = {
                "Content-Type": "application/json"
            }
            
            params_backup = {
                "apikey": api_key_backup
            }
            
            response_backup = requests.get(url_backup+company, headers=headers_backup, params=params_backup)
            
            if response_backup.status_code == 200:
                data_things = response_backup.json()
                for item in data_things['Global Quote']:
                    flattened_item = {
                        "symbol": company,
                        '‘Date': f'{datetime.datetime.now().day}/{datetime.datetime.now().month}/{datetime.datetime.now().year}',
                        'Open': item['02. open'],
                        'High': item['03. high'],
                        'Low': item['04. low'],
                        'Close': item['08. previous close'],
                        'Price': item['05. price'],
                        'Volume': item['06. volume']
                    }
                    
                    flattened_items.append(flattened_item)
            else:
                url_backup_backup = "https://api.tiingo.com/iex/?tickers="
            
                api_key_backup_backup = "2f0b4e349d4927446d26e3f29dc14b833f3aef5b"

                headers_backup_backup = {
                    "Content-Type": "application/json"
                }
                
                params_backup_backup = {
                    "token": api_key_backup_backup
                }
                
                response_backup_backup = requests.get(url_backup_backup+company, headers=headers_backup_backup, params=params_backup_backup)
                
                if response_backup_backup.status_code == 200:
                    data_things = response_backup_backup.json()
                    for item in data_things[0]:
                        flattened_item = {
                            "symbol": company,
                            '‘Date': f'{datetime.datetime.now().day}/{datetime.datetime.now().month}/{datetime.datetime.now().year}',
                            'Open': item['open'],
                            'High': item['high'],
                            'Low': item['low'],
                            'Close': item['prevClose'],
                            'Price': item['last'],
                            'Volume': item['volume']
                        }
                        
                        flattened_items.append(flattened_item)
    
    df = pd.DataFrame(flattened_items)
                    
    csv_data = df.to_csv(index=False)
                
    with fs.open(f"{gcs_bucket}/data/raw_data/share_price_{today_date}.csv", "w") as f:
        f.write(csv_data)

    logger.info(f"Get daily share price ended")
            
def get_earnings():
    url = "https://financialmodelingprep.com/api/v3/earnings-surprises/"

    api_key = "RDpyXbrQwU2B7p6iPxshEU7ezSX5ILdu"
    
    headers = { 
        "Content-Type": "application/json"
    } 

    params = {
        "apikey": api_key
    }

    flattened_items = []

    try: 
        for company in companies:
            response = requests.get(url+company, headers=headers, params=params) # Make an API request to get earnings data

            if response.status_code == 200:
                data_things = response.json()
                flattened_item = {
                    'symbol': data_things[0]['symbol'],
                    'dateâ€™': data_things[0]['date'],
                    'actualEarningResultâ€™ ': data_things[0]['actualEarningResult']
                }

                flattened_items.append(flattened_item)

            else:
                logger.error(f"Error: {response.status_code} - {response.text}")

                url_backup = "https://www.alphavantage.co/query?function=EARNINGS&symbol="
                api_key_backup = "3KU1BG4P5UTBMLZY"
                headers_backup = {
                    "Content-Type": "application/json"
                }
                params_backup = {
                    "apikey": api_key_backup
                }
                response_backup = requests.get(url_backup+company, headers=headers_backup, params=params_backup)
            
                if response.status_code == 200:
                    data_things = response.json()
                    flattened_item = {
                        'symbol': data_things['symbol'],
                        'fiscalDateEnding': data_things['annualEarnings'][0]['fiscalDateEnding'],
                        'reportedEPSâ€™  ': data_things['annualEarnings'][0]['reportedEPS']
                    }
                
                flattened_items.append(flattened_item)
            
        df = pd.DataFrame(flattened_items)
                
        csv_data = df.to_csv(index=False)
                    
        with fs.open(f"{gcs_bucket}/data/raw_data/earnings.csv", "w") as f:
            f.write(csv_data)

        logger.info("Earnings data successfully retrieved and uploaded.") # Log a success message

    except Exception as e:
        # Log an error if an exception occurs
        logger.error(f"Error occurred: {str(e)}")

def get_sentiment():
    
    url = "https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers="
    
    api_key = "7EM86RAEWREII1BH"

    headers = {
        "Content-Type": "application/json"
    }

    params = {
        "apikey": api_key
    }
    
    flattened_items = []
    
    logger.info(f"Starting to get sentiment")
    
    for company in companies:
        try:
            response = requests.get(url+company, headers=headers, params=params)

            logger.info(f"trying something")
            
            if response.status_code == 200:
                
                data = response.json()
                feed_items = data.get("feed", [])
                
                for item in feed_items:
                    flattened_item = {
                        "symbol": company,
                        "title": item.get("title"),
                        "url": item.get("url"),
                        "time_published": item.get("time_published"),
                        "authors": item.get("authors", []),
                        "summary": item.get("summary"),
                        "banner_image": item.get("banner_image"),
                        "source": item.get("source"),
                        "category_within_source": item.get("category_within_source"),
                        "source_domain": item.get("source_domain"),
                        "topics": item.get("topics", []),
                        "overall_sentiment_score": item.get("overall_sentiment_score"),
                        "overall_sentiment_label": item.get("overall_sentiment_label"),
                        "ticker_sentiments": [
                            {
                                "ticker": ts.get("ticker"),
                                "relevance_score": ts.get("relevance_score"),
                                "ticker_sentiment_score": ts.get("ticker_sentiment_score"),
                                "ticker_sentiment_label": ts.get("ticker_sentiment_label"),
                            }
                            for ts in item.get("ticker_sentiments", [])
                        ]
                    }
        
                    flattened_items.append(flattened_item)
    
                df = pd.DataFrame(flattened_items)
                
                csv_data = df.to_csv(index=False)
                
                with fs.open(f"{gcs_bucket}/data/raw_data/sentiment_{today_date}.csv", "w") as f:
                    f.write(csv_data)
                    
                logger.info(f"Get sentiment ended")

            else:
                logger.error(f"Error:", response.status_code)
                logger.error(response.text)

        except Exception as e:
            logger.error(f"Error occurred: {str(e)}")

def get_growth_rates():
    url = 'https://financialmodelingprep.com/api/v3/balance-sheet-statement/'

    api_key = "4eiHO5ke53QNwBQEO3zSPVMaH5kEiPJq"

    headers = {
        "Content-Type": "application/json"
    }

    params = {
        'period': 'annual',
        "apikey": api_key
    }

    flattened_items = []

    response = requests.get(url, headers=headers, params=params)

    logger.info(f"Starting to get growth rates")
    
    for company in companies:
        response = requests.get(url+company, headers=headers, params=params)

        if response.status_code == 200:
            data_things = response.json()
            for item in data_things:
                flattened_item = {
                "symbol": company,
                "date": item["date"],
                'totalAssets': item['totalAssets'],
                'totalCurrentAssets': item['totalCurrentAssets'],
                'cashAndCashEquivalents': item['cashAndCashEquivalents'],
                'shortTermInvestments': item['shortTermInvestments'],
                'inventory': item['inventory'],
                'totalCurrentLiabilities': item['totalCurrentLiabilities'],
                'longTermDebt': item['longTermDebt'],
                'retainedEarnings': item['retainedEarnings'],
                'commonStock': item['commonStock']
                }
                
                flattened_items.append(flattened_item)
        else:
        #AV
            url_backup = "https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol="
                
            api_key_backup = "7EM86RAEWREII1BH"

            headers_backup = {
                "Content-Type": "application/json"
            }
            
            params_backup = {
                "apikey": api_key_backup
            }
            
            response_backup = requests.get(url_backup+company, headers=headers_backup, params=params_backup)
                
            data_things = response_backup.json()
            for item in data_things['annualReports']:
                flattened_item = {
                    "symbol": company,
                    "date": item['fiscalDateEnding'],
                    'totalAssets': item['totalAssets'],
                    'totalCurrentAssets': item['totalCurrentAssets'],
                    'cashAndCashEquivalents': item['cashAndCashEquivalentsAtCarryingValue'],
                    'shortTermInvestments': item['shortTermInvestments'],
                    'inventory': item['inventory'],
                    'totalCurrentLiabilities': item['totalCurrentLiabilities'],
                    'longTermDebt': item['longTermDebt'],
                    'retainedEarnings': item['retainedEarnings'],
                    'commonStock': item['commonStock']
                }
                
                flattened_items.append(flattened_item)

    df = pd.DataFrame(flattened_items)
                    
    csv_data = df.to_csv(index=False)
                
    with fs.open(f"{gcs_bucket}/data/raw_data/growth_rates.csv", "w") as f:
        f.write(csv_data)

    logger.info(f"Get growth rates ended")

def get_income_statement():
    #FMP
    url = 'https://financialmodelingprep.com/api/v3/income-statement/'

    api_key = "4eiHO5ke53QNwBQEO3zSPVMaH5kEiPJq"

    headers = {
        "Content-Type": "application/json"
    }

    params = {
        'period': 'annual',
        "apikey": api_key
    }

    flattened_items = []

    logger.info(f"Starting to get income statement")
    
    response = requests.get(url, headers=headers, params=params)

    for company in companies:
        response = requests.get(url+company, headers=headers, params=params)

        if response.status_code == 200:
            data_things = response.json()
            for item in data_things:
                flattened_item = {
                "symbol": company,
                "date": item["date"],
                'incomeBeforeTax': item['incomeBeforeTax'],
                'netIncome': item['netIncome'],
                'interestIncome': item['interestIncome'],
                'interestExpense': item['interestExpense'],
                'depreciationAndAmortization': item['depreciationAndAmortization'],
                'grossProfit': item['grossProfit'],
                'operatingIncome': item['operatingIncome'],
                'revenue': item['revenue']
                }
                
                flattened_items.append(flattened_item)
        else:
        #AV
        
            url_backup = "https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol="
                
            api_key_backup = "7EM86RAEWREII1BH"

            headers_backup = {
                "Content-Type": "application/json"
            }
            
            params_backup = {
                "apikey": api_key_backup
            }
            
            response_backup = requests.get(url_backup+company, headers=headers_backup, params=params_backup)
                
            data_things = response_backup.json()
            for item in data_things['annualReports']:
                flattened_item = {
                    "symbol": company,
                    "date": item["fiscalDateEnding"],
                    'incomeBeforeTax': item['incomeBeforeTax'],
                    'netIncome': item['netIncome'],
                    'interestIncome': item['interestIncome'],
                    'interestExpense': item['interestExpense'],
                    'depreciationAndAmortization': item['depreciationAndAmortization'],
                    'grossProfit': item['grossProfit'],
                    'operatingIncome': item['operatingIncome'],
                    'revenue': item['totalRevenue']
                }
                
                flattened_items.append(flattened_item)
                
    df = pd.DataFrame(flattened_items)
                    
    csv_data = df.to_csv(index=False)
                
    with fs.open(f"{gcs_bucket}/data/raw_data/income_statement.csv", "w") as f:
        f.write(csv_data)

    logger.info(f"Get income statement ended")

def get_eps():
    #FMP
    url = 'https://financialmodelingprep.com/api/v3/quote/'

    api_key = "4eiHO5ke53QNwBQEO3zSPVMaH5kEiPJq"

    headers = {
        "Content-Type": "application/json"
    }

    params = {
        "apikey": api_key
    }

    flattened_items = []

    logger.info(f"Starting to get EPS")
    
    response = requests.get(url, headers=headers, params=params)

    for company in companies:
        response = requests.get(url+company, headers=headers, params=params)

        if response.status_code == 200:
            data_things = response.json()
            
            flattened_item = {
                "symbol": company,
                "eps": data_things[0]["eps"]
            }
                
            flattened_items.append(flattened_item)
        else:
        #AV
        
            url_backup = "https://www.alphavantage.co/query?function=EARNINGS&symbol="
                
            api_key_backup = "7EM86RAEWREII1BH"

            headers_backup = {
                "Content-Type": "application/json"
            }
            
            params_backup = {
                "apikey": api_key_backup
            }
            
            response_backup = requests.get(url_backup+company, headers=headers_backup, params=params_backup)
                
            data_things = response_backup.json()
            flattened_item = {
                "symbol": company,
                "eps": data_things['annualEarnings'][0]["reportedEPS"]
            }
                
            flattened_items.append(flattened_item)
                
    df = pd.DataFrame(flattened_items)
                    
    csv_data = df.to_csv(index=False)
                
    with fs.open(f"{gcs_bucket}/data/raw_data/eps.csv", "w") as f:
        f.write(csv_data)
    
    logger.info(f"Get EPS ended")

# Define the Airflow DAG and its configurations
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

# Create the DAG instance
extraction_dag = DAG(
    'extraction_dag',
    default_args=default_args, 
    description='Extract data from multiple API to GCS',
    schedule_interval="0 22 * * 1-5",
    catchup=False
)

# Define DAG tasks
start_dag = PythonOperator(
    task_id='start_dag',
    python_callable=start_dag,
    dag=extraction_dag
) 

get_profile = PythonOperator(
    task_id='get_profile',
    python_callable=get_profile,
    dag=extraction_dag
)

get_financial_profile = PythonOperator(
    task_id='get_financial_profile',
    python_callable=get_financial_profile,
    dag=extraction_dag
)

get_sentiment = PythonOperator(
    task_id='get_sentiment',
    python_callable=get_sentiment,
    dag=extraction_dag
)

get_growth_rates = PythonOperator(
    task_id='get_growth_rates',
    python_callable=get_growth_rates,
    dag=extraction_dag
)

get_income_statement = PythonOperator(
    task_id='get_income_statement',
    python_callable=get_income_statement,
    dag=extraction_dag
)

get_earnings = PythonOperator(
    task_id='get_earnings',
    python_callable=get_earnings,
    dag=extraction_dag
)

get_share_price = PythonOperator(
    task_id='get_share_price',
    python_callable=get_share_price,
    dag=extraction_dag
)

end_dag = PythonOperator(
    task_id='end_dag',
    python_callable=end_dag,
    dag=extraction_dag
)

# Define the task dependencies
start_dag >> get_profile >> get_financial_profile >> get_growth_rates >> get_income_statement >> get_earnings >> get_share_price >> get_sentiment >> end_dag