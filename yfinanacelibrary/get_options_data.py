
import yfinance as yf
import pandas as pd

def get_company_option_chain(tickers_objs, COMPANY_NAME, expiry_date, return_earnings_date=False):

    ticker = tickers_objs.tickers[COMPANY_NAME]

    try:
        ticker_last_value = ticker.history()['Close'].iloc[-1]
        # convert to float
        ticker_last_value = float(ticker_last_value)
        #print(ticker_last_value)
        

        ticker_options_chain = ticker.option_chain(expiry_date)

        # For calls plot a graph with the price and th extrinsic value
        call_df = ticker_options_chain.calls.copy()
        # FIlter only the necessary rows (strike price is within +-40% of the last value) and columns (strike, lastPrice, bid, ask)
        call_df = call_df[(call_df["strike"] > (ticker_last_value * 0.5)) & (call_df["strike"] < (ticker_last_value * 1.05 ))]
        call_df = call_df[["strike", "lastPrice", "bid", "ask"]]

        # plot intrinsic value Vs chain price
        # intrensic value is the "ticker_last_value - strike price" and extrinsic value is the "(ask - bid)/2"

        call_df["intrinsic"] = ticker_last_value - call_df["strike"]
        call_df["extrinsic"] = call_df["bid"]
        call_df["extrinsic_gain"] = call_df["extrinsic"] - call_df["intrinsic"]
        call_df["extrinsic_gain_ratio"] = (call_df["extrinsic_gain"] / call_df["strike"]) * 100 # if less than 0, make it 0
        call_df["extrinsic_gain_ratio"] = call_df["extrinsic_gain_ratio"].apply(lambda x: 0 if x < 0 else x)
        # round off to 3 decimal places
        call_df = call_df.round(3)
        call_df

        last_value_90 = ticker_last_value * 0.9
        call_df_90 = call_df[(call_df["strike"] > last_value_90)]
        extrinsic_callgain_ratio_at_90 = call_df_90.iloc[0]["extrinsic_gain_ratio"]
        option_price = call_df_90.iloc[0]["extrinsic_gain"] * 100

        put_df = ticker_options_chain.puts.copy()
        # FIlter only the necessary rows (strike price is within +-40% of the last value) and columns (strike, lastPrice, bid, ask)
        put_df = put_df[(put_df["strike"] > (ticker_last_value * 0.5)) & (put_df["strike"] < (ticker_last_value * 1.05 ))]
        put_df = put_df[["strike", "lastPrice", "bid", "ask"]]

        put_df['extrinsic_gain_ratio'] = (put_df['bid'] / put_df['strike']) * 100
        put_df['extrinsic_gain_ratio'] = put_df['extrinsic_gain_ratio'].apply(lambda x: 0 if x < 0 else x)
        put_df = put_df.round(3)
        put_df

        put_df_90 = put_df[(put_df["strike"] > last_value_90)]
        extrinsic_putgain_ratio_at_90 = put_df_90.iloc[0]["extrinsic_gain_ratio"]

        # find the row with NAN in the earnings date
        earnings_date = ""
        if return_earnings_date:
            ticker = yf.Ticker(COMPANY_NAME)
            earnings_date_df =ticker.earnings_dates
            earnings_date_df = earnings_date_df[earnings_date_df['Reported EPS'].isnull()]
            # find the last date in the earnings date
            earnings_date = earnings_date_df.iloc[-1].name
            # find the date value from the name
            earnings_date = earnings_date.date()
            #print(earnings_date)

        return call_df, put_df, ticker_last_value, earnings_date, extrinsic_callgain_ratio_at_90, extrinsic_putgain_ratio_at_90, option_price
    
    except Exception:
        # print(f"Error in getting the call options for {COMPANY_NAME}, {e}")
        return None, None, None, None, None, None, None