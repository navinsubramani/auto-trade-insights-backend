import yfinance as yf
import pandas as pd

from yfinanacelibrary.get_index_metadata import get_index_metadata
from yfinanacelibrary.get_stock_historicdata import get_stock_historicdata
from yfinanacelibrary.compute_stock_data import get_normalized_stockdata, get_stock_momentum_data
from yfinanacelibrary.get_options_data import get_company_option_chain

def query_stock_information(COMAPNY_NAME):
    # Get the stock information
    stock = yf.Ticker(COMAPNY_NAME)

    try:
        # get all stock info
        news = stock.news
        news_link = []
        for i in news:
            data = {
                "title": i['title'],
                "link": i['link']
            }
            news_link.append(data)

        info = stock.info
        data = {
            "sector": info['sector'],
            "forwardPE": info['forwardPE'],
            "dividendYield": info['dividendYield'],
            "averageVolume": info['averageVolume'],
            "fiftyTwoWeekHigh": info['fiftyTwoWeekHigh'],
            "fiftyTwoWeekLow": info['fiftyTwoWeekLow'],
            "news": news_link
        }

        # last 7 day movements
        hist = stock.history(period="1mo")
        # get last five rows
        hist = hist.tail(5)

        # first element open - last elemnt close
        movement = hist.iloc[-1]['Close'] - hist.iloc[0]['Open']
        data['7_day_open_price'] = hist.iloc[0]['Open']
        data['movement'] = float(movement).__round__(2)
        return data
    
    except Exception as e:
        return {
            "sector": "N/A",
            "forwardPE": "N/A",
            "dividendYield": "N/A",
            "averageVolume": "N/A",
            "fiftyTwoWeekHigh": "N/A",
            "fiftyTwoWeekLow": "N/A",
            "news": [],
            "7_day_open_price": "N/A",
            "movement": "N/A"
        }


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
    

def query_options_data():

    # Read the Sector details of the companies
    sector_list = pd.read_csv('index_company_details.csv')
    sector_list = sector_list[["Company", "Updated Sector"]]

    # Create a space separated string of the tickers from the dataframe
    tickers = " ".join(sector_list["Company"].values)
    tickers_objs = yf.Tickers(tickers)

    # Find the next expiry date for AAPL, and have that as a standard for all stocks expiry date
    ticker = yf.Ticker('AAPL')
    ticker_option_dates = ticker.options
    expiry_date = ticker_option_dates[0]
    # print(expiry_date)

    # Initialize array for storing the covered call gains
    covered_sell_gains = {}
    call_option_chain = {}
    put_option_chain = {}

    COMPANY_LIST = sector_list["Company"].values

    for COMPANY_NAME in COMPANY_LIST:
        # Get the covered call gains for the company
        call_df, put_df, ticker_last_value, earnings_date, extrinsic_gain_ratio_at_90, extrinsic_putgain_ratio_at_90, option_price  = get_company_option_chain(tickers_objs, COMPANY_NAME, expiry_date, return_earnings_date=False)
        # print(f"Company: {COMPANY_NAME}, Last Value: {ticker_last_value}, Extrinsic gain at 90%: {extrinsic_gain_ratio_at_90}, Expiry Date: {expiry_date}")
        covered_sell_gains[COMPANY_NAME] = { 
            "Last Value": ticker_last_value,
            "Earnings Date": earnings_date,
            "Extrinsic sell call gain at 90%": extrinsic_gain_ratio_at_90,
            "Extrinsic sell put gain at 90%": extrinsic_putgain_ratio_at_90,
            "Option Price": option_price
        }
        call_option_chain[COMPANY_NAME] = call_df
        put_option_chain[COMPANY_NAME] = put_df

    # Convert into a dataframe
    covered_sell_gains_df = pd.DataFrame(covered_sell_gains).T

    # Show the covered call gains in descending order
    # remove None values
    covered_sell_gains_df = covered_sell_gains_df.dropna()
    covered_sell_gains_df = covered_sell_gains_df.sort_values(by="Extrinsic sell call gain at 90%", ascending=False)

    # show only the concerned gains top 10
    # covered_call_gains_df = covered_call_gains_df.head(10)

    return covered_sell_gains_df


def query_options_data_for_single_stock(TICKER_NAME):

    tickers_objs = yf.Tickers(TICKER_NAME)

    # Find the next expiry date for AAPL, and have that as a standard for all stocks expiry date
    ticker = yf.Ticker('AAPL')
    ticker_option_dates = ticker.options
    expiry_date = ticker_option_dates[0]
    # print(expiry_date)

    # Initialize array for storing the covered call gains
    covered_sell_gains = {}

    # Get the covered call gains for the company
    call_df, put_df, ticker_last_value, earnings_date, extrinsic_gain_ratio_at_90, extrinsic_putgain_ratio_at_90, option_price  = get_company_option_chain(tickers_objs, TICKER_NAME, expiry_date, return_earnings_date=True)
    # print(f"Company: {COMPANY_NAME}, Last Value: {ticker_last_value}, Extrinsic gain at 90%: {extrinsic_gain_ratio_at_90}, Expiry Date: {expiry_date}")

    information = query_stock_information(TICKER_NAME)
    information["Company"] = TICKER_NAME
    information["Last Value"] = ticker_last_value
    information["Earnings Date"] = earnings_date

    # If the dictionary has non JSON compatible values, convert them to string
    for key in information:
        if type(information[key]) not in [int, float, str, list]:
            information[key] = str(information[key])

    return call_df, put_df, information
