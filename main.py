from fastapi import FastAPI, Response
from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager
import pandas as pd
import requests
from datetime import datetime
import io

from yfinanacelibrary.query_compute_store_data import query_compute_store_data

app = FastAPI()

# Initialize the scheduler
scheduler = BackgroundScheduler()

# Temporary in-memory storage for stock data
indexs_metadata = {}
stock_data = pd.DataFrame()

# Function to fetch stock data
def fetch_market_data():
    global stock_data
    global indexs_metadata
    try:
        indexs_metadata, stock_data = query_compute_store_data()

    except requests.RequestException as e:
        print(f"Error fetching stock data: {e}")


def fetch_market_data_duringmarkethours():
    # Fetch market data every 1 minute during market hours
    # Run the function only during the week day and 9AM to 5PM EDT
    #print(datetime.now().weekday(), datetime.now().hour)
    if datetime.now().weekday() < 5 and datetime.now().hour >= 9 and datetime.now().hour < 17:
        fetch_market_data()


# Lifespan context manager for startup and shutdown events
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start scheduler and fetch initial data on startup
    fetch_market_data()  # Fetch initial data
    scheduler.add_job(fetch_market_data_duringmarkethours, "interval", minutes=1)  # Run every 1 minute
    scheduler.start()
    
    try:
        yield  # Allows the application to run

    finally:
        # Shutdown the scheduler on shutdown
        scheduler.shutdown(wait=False)

# Apply the lifespan context manager to the app
app.router.lifespan_context = lifespan


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

# Run the app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
