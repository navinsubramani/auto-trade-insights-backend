import yfinance as yf
import pandas as pd

from yfinanacelibrary.get_index_metadata import get_index_metadata
from yfinanacelibrary.get_stock_historicdata import get_stock_historicdata
from yfinanacelibrary.compute_stock_data import get_normalized_stockdata, get_stock_momentum_data


def query_compute_store_data():

    # Initialize the index and stock period and interval
    INDEXS = ["SPY", "QQQ"]
    STOCK_PERIOD = "5d"
    STOCK_INTERVAL = "1m"

    # Get the list of companies in the index and their weightage
    indexs_metadata = {}
    for index in INDEXS:
        index_list, stocks_total_weightage, cash_total_weightage = get_index_metadata(index)
        # index_list is a dataframe with columns: "INDEX Company", "INDEX Weight", "Sector"
        indexs_metadata[index] = {"index_list": index_list, "stocks_total_weightage": stocks_total_weightage, "cash_total_weightage": cash_total_weightage}
    
    # Get the historical data of all the stocks from all the indexes
    # find the unique companies in all the indexes
    companies_list = []
    for index in INDEXS:
        companies_list.extend(indexs_metadata[index]["index_list"][index+" Company"].tolist())
    companies_list.extend(INDEXS)
    companies_list = list(set(companies_list))

    # Get the historical data of the stocks
    stock_data = get_stock_historicdata(companies_list, STOCK_PERIOD, STOCK_INTERVAL)

    # Normalize the stock data value per index (essentially the value of each stock within a index)

    for index in INDEXS:
        index_list = indexs_metadata[index]["index_list"]
        cash_total_weightage = indexs_metadata[index]["cash_total_weightage"]

        normalized_stockdata = get_normalized_stockdata(index, index_list, stock_data, cash_total_weightage)
        indexs_metadata[index]["normalized_stockdata"] = normalized_stockdata

    # Calculate the momentum of the stocks
    stock_momentum_data = get_stock_momentum_data(stock_data.iloc[::-1])

    return indexs_metadata, stock_data, stock_momentum_data
    