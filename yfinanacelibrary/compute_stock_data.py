import pandas as pd



def get_normalized_stockdata(INDEX, index_list, stock_data, cash_total_weightage):

    # calcualte the normalizer value
    # find the last row of the data so we can calculate the multiplication factor
    last_row = stock_data.tail(1)
    index_lastvalue = last_row["Adj Close"][INDEX].values[0]
    index_normalizer = index_lastvalue / 100 # Assumption is that this is normalizer

    # Initialize dataframes to store the weighted data
    temp_weighted_data = {}
    for i, row in index_list.iterrows():
        temp_ticker = row[INDEX+" Company"]
        temp_weight = row[INDEX+" Weight"]
        temp_ticker_lastvalue = stock_data["Adj Close"][temp_ticker].tail(1).values[0]
        temp_weighted_data[temp_ticker] = stock_data["Adj Close"][temp_ticker] * ( temp_weight * index_normalizer ) / temp_ticker_lastvalue

    weighted_data = pd.DataFrame(temp_weighted_data)

    # Add the companies normalized value based on last value and calculate the total sum accouting the cash value
    weighted_data["Normalized "+INDEX] = weighted_data.sum(axis=1) + (cash_total_weightage*index_normalizer)
    weighted_data[INDEX] = stock_data["Adj Close"][INDEX]
    return weighted_data