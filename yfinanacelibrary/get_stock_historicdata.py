import yfinance as yf
import pandas as pd

STOCK_PERIOD = "5d"
STOCK_INTERVAL = "1m"

def get_stock_historicdata(COMPANY_LIST, STOCK_PERIOD, STOCK_INTERVAL):
    """
    This function get the historical data of the stocks in the index and returns it.
    """

    # Create a space separated string of the tickers from the dataframe
    tickers = " ".join(COMPANY_LIST)

    # get the historical data for the tickers in a for loop and append the data to a dataframe with the ticker as the column name   

    # download the historical data for the tickers
    index_data = yf.download(tickers, period=STOCK_PERIOD, interval=STOCK_INTERVAL, threads=False)

    # If the data has a NAN value, use the previous row value to fill it (backward fill)
    index_data = index_data.bfill()
    index_data = index_data.ffill()

    # Reverse the order of the rows in the dataframe
    index_data = index_data.iloc[::-1]

    # Store the data in a CSV file
    # index_data.to_csv('index_raw_data.csv')

    return index_data