from fastapi import FastAPI, Response
from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager
import pandas as pd
import requests
from datetime import datetime
import io

from yfinanacelibrary.query_compute_store_data import query_compute_store_data, query_options_data, query_options_data_for_single_stock

app = FastAPI()

# Initialize the scheduler
scheduler = BackgroundScheduler()

# Temporary in-memory storage for stock data
indexs_metadata = {}
stock_data = pd.DataFrame()
momentum_data = pd.DataFrame()
sell_options_data = pd.DataFrame()

# Function to fetch stock data
def fetch_market_data():
    global stock_data
    global indexs_metadata
    global momentum_data
    try:
        indexs_metadata, stock_data, momentum_data = query_compute_store_data()

    except requests.RequestException as e:
        print(f"Error fetching stock data: {e}")


def fetch_market_data_duringmarkethours():
    # Fetch market data every 1 minute during market hours
    # Run the function only during the week day and 9AM to 5PM EDT
    # 9AM to 5PM EDT is 1PM to 9PM UTC & Weekdays
    current_time = datetime.now()
    if (current_time.weekday() < 5) and (current_time.hour >= 13) and (current_time.hour < 21):
        fetch_market_data()


def fetch_options_data():
    global sell_options_data
    # Fetch options data
    try:
        sell_options_data = query_options_data()

    except requests.RequestException as e:
        print(f"Error fetching options data: {e}")


def fetch_options_data_duringmarkethours():
    # Fetch options data every 1 minute during market hours
    # Run the function only during the week day and 9AM to 5PM EDT
    # 9AM to 5PM EDT is 1PM to 9PM UTC & Weekdays
    current_time = datetime.now()
    if (current_time.weekday() < 5) and (current_time.hour >= 13) and (current_time.hour < 21):
        fetch_options_data()

# Lifespan context manager for startup and shutdown events
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start scheduler and fetch initial data on startup
    fetch_market_data()  # Fetch initial data
    print("Startup stock data is fetched")
    fetch_options_data()  # Fetch initial data
    print("Startup options data is fetched")
    scheduler.add_job(fetch_market_data_duringmarkethours, "interval", minutes=3)  # Run every 3 minute
    scheduler.add_job(fetch_options_data_duringmarkethours, "interval", minutes=5)  # Run every 5 minute
    scheduler.start()
    
    try:
        yield  # Allows the application to run

    finally:
        # Shutdown the scheduler on shutdown
        scheduler.shutdown(wait=False)

# Apply the lifespan context manager to the app
app.router.lifespan_context = lifespan


# endpoint for the main page
@app.get("/")
def read_root():
    return {
        "message": "Welcome to the Stock Market Data API. Use the /stock endpoint to get stock data and /indexdata/{index}/metadata endpoint to get index metadata."
    }

# Endpoint to get stock data
@app.get("/stock")
def get_stock_data():
    # convert the output to json where the index is the key and the value is the normalized stock data
    output = stock_data.iloc[::-1]
    buffer = io.StringIO()
    output.to_csv(buffer, index=True)
    buffer.seek(0)
    return Response(content=buffer.getvalue(), media_type="text/csv")

# Endpoint to get index metadata for the given index data
@app.get("/indexdata/{index}/metadata")
def get_index_metadata(index: str):
    return indexs_metadata[index]["index_list"]

@app.get("/indexdata/{index}/normalizedstockdata")
async def get_normalized_stockdata(index: str):
    # convert the output to json where the index is the key and the value is the normalized stock data
    output = indexs_metadata[index]["normalized_stockdata"].iloc[::-1]
    buffer = io.StringIO()
    output.to_csv(buffer, index=True)
    buffer.seek(0)
    return Response(content=buffer.getvalue(), media_type="text/csv")

@app.get("/stockmomentumdata")
async def get_stock_momentum_data():
    # convert the output to json where the index is the key and the value is the normalized stock data
    output = momentum_data
    buffer = io.StringIO()
    output.to_csv(buffer, index=True)
    buffer.seek(0)
    return Response(content=buffer.getvalue(), media_type="text/csv")

@app.get("/selloptionsdata")
async def get_sell_options_data():
    # convert the output to json where the index is the key and the value is the normalized stock data
    output = sell_options_data
    buffer = io.StringIO()
    output.to_csv(buffer, index=True)
    buffer.seek(0)
    return Response(content=buffer.getvalue(), media_type="text/csv")

@app.get("/stockoptionsdata/{company}")
async def get_stock_options_data(company: str):
    # convert the output to json where the index is the key and the value is the normalized stock data
    call_df, put_df, info = query_options_data_for_single_stock(company)

    response = {
        "Call Options": call_df,
        "Put Options": put_df,
        "Information": info
    }

    return response


# Run the app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
