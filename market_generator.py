import random
import pandas as pd # package imported for dataframe analysis
import ccxt # imported library to fetch trading pair data
import time


# We want to get data from Binance exchange. This instantiates a class.

binance_exchange = ccxt.binance()

def get_random_market():
    """
    This function returns a random market/trading pair.
    """
    all_binance_markets = list(binance_exchange.load_markets().keys())
    return random.choice(all_binance_markets)


def fetch_all_ohlcv(symbol,total_limit,batch_limit,timeframe='1m', since=None):
    
    """
    Fetch all historical OHLCV data for a given symbol and timeframe from Binance.
    """
    all_ohlcv = []
    while len(all_ohlcv) < total_limit:
        ohlcv = binance_exchange.fetch_ohlcv(symbol, timeframe, since, limit=batch_limit)
        if not ohlcv:  # Stop if no more data is available
            break
        all_ohlcv.extend(ohlcv)
        since = ohlcv[-1][0] + 1  # Update 'since' to fetch next batch
        time.sleep(binance_exchange.rateLimit / 1000)  
    return all_ohlcv[:total_limit]  


def fetch_required_markets(no_of_markets, desired_data_points):

    """
    Function that returns a specific number of markets (trading pairs) to be 
    analysed and possibly traded in the following analysis.
    """

    data_dict = {}
    selected_markets = set()

    while len(selected_markets) < no_of_markets:
        market = get_random_market()
        if market in selected_markets:
            continue

        print(f"Fetching data for {market}")
        data = fetch_all_ohlcv(market, total_limit=desired_data_points, batch_limit=1000, since=binance_exchange.parse8601('2023-01-01T00:00:00Z'))
        if len(data) >= desired_data_points:
            df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            data_dict[market] = df
            selected_markets.add(market)
            print(f"Data for {market} fetched successfully")
        else:
            print(f"Insufficient data for {market}, skipping")

    return data_dict, list(selected_markets)


# Desired number of markets and data points. Effectively we attach one month long of minute resolution data.

desired_no_of_markets = 50
desired_data_points = 43800

# Fetch the required data

data_dict, selected_markets = fetch_required_markets(desired_no_of_markets, desired_data_points)

# Store the data in the location that this .py runs for offline analysis. 
# This could be commented off and all the procedure could be run in a sinly .py or .ipynb
# but the modularity and storage for future analyses would be suboptimal.


for pair, df in data_dict.items():
    df.to_csv(f"{pair.replace('/', '_')}_data.csv", index=False)
    print(f"Data for {pair} saved to CSV")

