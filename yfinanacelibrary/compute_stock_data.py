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

def get_stock_momentum_data(stock_data):
    momentum_window = 30
    recent_window = 30

    # Calculate the rate of change (momentum) over a 5-day window
    momentum = stock_data['Adj Close'].pct_change(periods=momentum_window) * 100
    momentum = momentum.reset_index(drop=True)

    # Find the peak momentum value in the recent window
    absolute_momentum = abs(momentum)
    recent_peak_momentum_index = absolute_momentum.iloc[-recent_window:].idxmax()

    # Find the peak value
    recent_peak_momentum = {}
    peak_value = {}
    momentum_after_peak = {}
    momentum_duration_after_peak = {}

    for company in momentum.columns:
        recent_peak_momentum[company] = momentum[company].iloc[recent_peak_momentum_index[company]]

        peak_value[company] = stock_data['Adj Close'][company].iloc[recent_peak_momentum_index[company]]
        
        temp_peak_index = recent_peak_momentum_index[company]
        temp_peak_value = stock_data['Adj Close'][company].iloc[temp_peak_index]
        temp_last_value = stock_data['Adj Close'][company].iloc[-1]
        momentum_after_peak[company] = (temp_last_value - temp_peak_value) / temp_peak_value * 100

        momentum_duration_after_peak[company] = len(stock_data['Adj Close'][company]) - temp_peak_index

    recent_peak_momentum = pd.Series(recent_peak_momentum)
    peak_value = pd.Series(peak_value)
    momentum_after_peak = pd.Series(momentum_after_peak)
    momentum_duration_after_peak = pd.Series(momentum_duration_after_peak)


    # Find the median volume for each stock
    median_volume = stock_data['Volume'].median()
    average_volume = stock_data['Volume'].mean()
    recent_volume = stock_data['Volume'].iloc[-20:].mean()


    # Find stocks with high recent momentum and lower current momentum
    settled_stocks = (abs(recent_peak_momentum) > 0.5) & (abs(momentum_after_peak) < abs(recent_peak_momentum) * 0.2) & (momentum_duration_after_peak > 10)
    unsettled_stocks = (abs(recent_peak_momentum) > 0.5) & ((abs(momentum_after_peak) > abs(recent_peak_momentum) * 0.4) | (momentum_duration_after_peak < 10))

    summarized_data = pd.DataFrame({
        "Recent Momentum": recent_peak_momentum,
        "Current Momentum": momentum_after_peak,
        "Peak": peak_value,
        "Settled": settled_stocks,
        "Settled Duration": momentum_duration_after_peak,
        "Unsettled": unsettled_stocks,
        "Recent Volume": recent_volume,
        "Median Volume": median_volume,
        "Average Volume": average_volume
    })

    return summarized_data