import sys
import numpy as np
import pandas as pd 
from datetime import datetime, timedelta

def transform_prices_to_DataFrame(prices: str, print_out=False)  -> pd.DataFrame:
    """Accepts a flat file (.csv) containing unpivoted price data and transforms into Pandas DataFrame containing prices for ticker by column
    
    Args:
        prices (str): name of .csv file containing unpivoted price data with columns date, ticker and close_price

    Returns:
        df_prices (pd.DataFrame): Pandas DataFrame containing prices for ticker by column
    """
    
    #Import prices     
    try:
        df_prices_raw = pd.read_csv(f'{prices}')
    except:
        raise RuntimeError(f"Could not read {prices}.")

    #Validate headers
    df_headers_expected = ["date", "ticker", "close_price"]

    for header in df_prices_raw.columns:
        if header in df_headers_expected:
            pass
        else:
            raise ValueError(f"{header} is not a valid column name. Expected: {df_headers_expected}")

    df_prices = pd.pivot_table(
        df_prices_raw, 
        values='close_price', 
        index=['date'], 
        columns='ticker'
     )

    #Convert index to datetime to allow for slicing later on
    df_prices.index = pd.to_datetime(df_prices.index)

    if print_out == True:
        print(df_prices)

    return df_prices


def combine_changed_tickers(df_prices: pd.DataFrame, old_ticker: str, new_ticker: str, effective_date: str, print_out=False) -> pd.Series:
    """Takes time series with an old ticker and time series with a a new ticker and concatenates them on an effective date. 

    Args:
        df_prices (pd.DataFrame): DataFrame containing index prices by ticker by column
        old_ticker (str): Previous ticker
        new_ticker (str): New ticker
        effective_date (str): Date on which change from old to new ticker took place

    Returns:
        pd.Series: Combined time series of prices from old_ticker up to effective date and new_ticker from effective date
    """

    df_all_prices = df_prices
    df_series_old_ticker = df_all_prices[old_ticker][:effective_date][:-1] 
    df_series_new_ticker = df_all_prices[new_ticker][effective_date:]
    df_series = pd.concat([df_series_old_ticker, df_series_new_ticker])

    if print_out == True:
        print(df_series)
   
    return df_series


if __name__ == "__main__":
   
   #Load and transform price data
    df_prices = transform_prices_to_DataFrame(prices="prices.csv", print_out=False)
    df_prices = df_prices[:"2024-12-31"]
    #df_prices.to_csv('df_prices.csv')

    ### Fetch and validate inputs ###  
    if len(sys.argv) < 2:
        raise ValueError("You must include a ticker and timeframe as inputted arguments when running this program.")
    
    input_ticker = sys.argv[1]
    input_timeframe = sys.argv[2]

    if input_ticker not in df_prices.columns:
        raise ValueError(f"Price data not found for ticker: {input_ticker}")

    dict_timeframes = {
        "1 day": 1,
        "5 days": 5,
        "6 months": 183,                # Rounding up
        "1 year": 365
    }

    try: 
        timeframe = dict_timeframes[input_timeframe]
        #print(f"Fetching return over {timeframe} days.")
    except:
        raise ValueError(f"{input_timeframe} is not a valid timeframe. Valid timeframes include: {[k for k,v in dict_timeframes.items()]}")

    beginning_date = datetime.strptime("2024-12-31", '%Y-%m-%d').date() + timedelta(days=-timeframe)

    ### Import Data ###
    #Import ticker change data
    try:
        df_ticker_changes = pd.read_csv('ticker_changes.csv')
    except:
        raise RuntimeError("Could not import 'ticker_changes.csv'.")    
    
    
    #Import split data
    try:
        df_splits = pd.read_csv('splits.csv')
    except:
        raise RuntimeError("Could not import 'splits.csv'.")    


    ### Account for Ticker Changes ### 
    #Elif statement links old ticker time series with new ticker time series so that entire series is returned irrespective of which is provided
    #Ticker provided is ticker pre change
    if input_ticker in df_ticker_changes['old_ticker'].values:
        df_ticker_changes_filtered = df_ticker_changes[df_ticker_changes["old_ticker"] == input_ticker]
        effective_date_ticker = df_ticker_changes_filtered['effective_date'].values[0]
        old_ticker = input_ticker
        new_ticker = df_ticker_changes_filtered['new_ticker'].values[0]
        df_series = combine_changed_tickers(df_prices, old_ticker, new_ticker, effective_date_ticker, print_out=False)
    
    #Ticker provided is ticker post change
    elif input_ticker in df_ticker_changes['new_ticker'].values:
        df_ticker_changes_filtered = df_ticker_changes[df_ticker_changes["new_ticker"] == input_ticker]
        effective_date_ticker = df_ticker_changes_filtered['effective_date'].values[0]
        old_ticker = df_ticker_changes_filtered['old_ticker'].values[0]
        new_ticker = input_ticker
        df_series = combine_changed_tickers(df_prices, old_ticker, new_ticker, effective_date_ticker, print_out=False)
    
    #No change to Ticker
    else:
        df_series = df_prices[input_ticker]
        old_ticker, new_ticker = input_ticker, input_ticker 
    
    
    ### Account for Stock Splits ### 
    #Figure out whether inputted series is subject to a stock split
    tickers = list(set([old_ticker, new_ticker]))
    
    for t in tickers:
        if t in df_splits['ticker'].values:             # Stock Split
            df_split_filtered = df_splits[df_splits["ticker"] == t]
            effective_date_split = df_split_filtered['effective_date'].values[0]    
            split_ratio = df_split_filtered['to_quantity'].values[0] / df_split_filtered['from_quantity'].values[0]
        
            #Where split date has transpired, multiply price by split ratio
            df_series = pd.Series(np.where(df_series.index >= effective_date_split, df_series * split_ratio, df_series), index=df_series.index)

        else:
           pass                     # No Stock Split


    ### Compute Return ###
    df_series = df_series[beginning_date:]
    t1 = df_series.iloc[-1]
    t0 = df_series.iloc[0]
    r = round(((t1 / t0)-1) * 100, 2)
    print(f"Return over {timeframe} days was {r}%.")

