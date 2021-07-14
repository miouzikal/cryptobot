import pandas as pd
import numpy as np
from binance.client import Client
from binance.exceptions import BinanceAPIException
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import itertools as it
import os
import json
import math
import time
from datetime import datetime, timezone, timedelta

binance_api_key = ""
binance_api_secret_key = ""
first_n_coins = 250
top_n_ranked_coins = 100
correlation_greater_than = 0.70
correlation_less_than = 1
paired_coin = "BTC"
history_end = datetime.now().astimezone(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
#history_end = (datetime.now().astimezone(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)) - timedelta(days = 1)
#history_end = datetime.now().replace(tzinfo=timezone.utc).astimezone(tz=None).replace(hour=0, minute=0, second=0, microsecond=0)
history_delta = 7
history_start = None
history_interval = Client.KLINE_INTERVAL_1MINUTE
coin_history_file = 'historical_klines.json'
used_coins_file = 'used_coins'
ignored_coins_file = 'ignored_coins'
client = Client()

def get_coins_from_file(file):
    supported_coin_list = []

    if os.path.exists(file):
        with open(file) as rfh:
            for line in rfh:
                line = line.strip()
                if not line or line.startswith("#") or line in supported_coin_list:
                    continue
                supported_coin_list.append(line)
    else:
        raise Exception("Coin list not found")

    return supported_coin_list


def get_all_tickers(bridge):
    coins = []
    
    for ticker in client.get_all_tickers():
        if bridge in ticker['symbol'] and ticker['symbol'].replace(bridge, "") in get_coins_from_file(used_coins_file):
            coins.append(ticker['symbol'].replace(bridge, ''))
    return coins


def klines_to_df(klines):
    df = pd.DataFrame.from_records(klines, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                   'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
    df['change'] = df.apply(lambda row: (
        (float(row.close) - float(row.open))/float(row.open))*100, axis=1)
    df['normalized'] = (df['close'].astype('float') - df['close'].astype('float').min()) / \
        (df['close'].astype('float').max()-df['close'].astype('float').min())
    return df


def pearson_correlation(x, y):
    lenght = len(x) if len(x) <= len(y) else len(y)
    meanx = sum(x)/lenght
    meany = sum(y)/lenght

    num = 0
    for i in range(lenght):
        num += ((x[i]-meanx)*(y[i]-meany))

    denx = 0
    deny = 0
    for i in range(lenght):
        denx += pow(x[i]-meanx, 2)
        deny += pow(y[i]-meany, 2)

    den = math.sqrt(denx*deny)

    return num/den


def get_all_coins_combinations(coin_list):
    filtered_coin_list = []
    combinations = []

    for coin in coin_list:
        filtered_coin_list.append(coin)

    for combination in list(it.product(filtered_coin_list, repeat=2)):
        if(combination[0] != combination[1]):
            combinations.append(combination)

    output = set(map(lambda x: tuple(sorted(x)), combinations))

    return output


def get_one_coin_combinations(coin_list, coin):
    combinations = []
    for c in coin_list:
        if(c != coin):
            combinations.append((c, coin))

    return combinations


def get_coin_history(coin, bridge):
    coin_kline = {}

    end = str(history_end.replace(microsecond=0).replace(tzinfo=timezone.utc).astimezone(tz=None))
    start = str(history_start.replace(microsecond=0).replace(tzinfo=timezone.utc).astimezone(tz=None))

    try:
        coin_kline[coin] = client.get_historical_klines(
            coin+bridge, history_interval, start, end)
    except BinanceAPIException as e:
        print("Error"+str(e))
        pass

    return coin_kline


def get_existing_coins(coin_list, coins_history):
    existing_coins = []
    for coin in coin_list:
        if coin in coins_history:
            existing_coins.append(coin)
    return existing_coins


def get_one_correlated_values(correlated_coin):
    verify_coins_files()

    coins_history = read_coins_history_file()
    ignored_coins = get_coins_from_file(
        ignored_coins_file) if os.path.isfile(ignored_coins_file) else []

    coin_list = []
    [coin_list.append(x) for x in get_coins_from_file(
        used_coins_file)[:first_n_coins] if x in coins_history and x not in ignored_coins]

    if correlated_coin not in coins_history:
        raise Exception("Coin not found")

    correlations = []
    sorted_correlations = {}

    combinations = get_one_coin_combinations(
        coin_list, correlated_coin)

    for coins in combinations:
        correlations.append({"coin_a": coins[0], "coin_b": coins[1], "correlation": pearson_correlation(
            coins_history[coins[0]]['normalized'].tolist(), coins_history[coins[1]]['normalized'].tolist())})

    filtered_correlations = [
        c for c in correlations if c['correlation'] > correlation_greater_than and c['correlation'] <= correlation_less_than]
    sorted_correlations = sorted(
        filtered_correlations, key=lambda i: i['correlation'])

    for c in sorted_correlations:
        print(c['coin_a']+"/"+c['coin_b']+": "+str(round(c['correlation'], 2)))


def get_one_correlated_list(correlated_coin, history):
    verify_coins_files(history)

    coins_history = read_coins_history_file(history)
    ignored_coins = get_coins_from_file(
        ignored_coins_file) if os.path.isfile(ignored_coins_file) else []

    coin_list = []
    [coin_list.append(x) for x in get_coins_from_file(
        used_coins_file)[:first_n_coins] if x in coins_history and x not in ignored_coins]

    if correlated_coin not in coins_history:
        raise Exception("Coin not found")

    correlations = []
    sorted_correlations = {}

    combinations = get_one_coin_combinations(
        coin_list, correlated_coin)

    for coins in combinations:
        correlations.append({"coin_a": coins[0], "coin_b": coins[1], "correlation": pearson_correlation(
            coins_history[coins[0]]['normalized'].tolist(), coins_history[coins[1]]['normalized'].tolist())})

    filtered_correlations = [
        c for c in correlations if c['correlation'] > correlation_greater_than and c['correlation'] <= correlation_less_than]
    sorted_correlations = sorted(
        filtered_correlations, key=lambda i: i['correlation'])

    correlated_coin_list = []
    filtered_correlated_coin_list = []

    for c in sorted_correlations:
        correlated_coin_list.append(c['coin_a'])
        correlated_coin_list.append(c['coin_b'])

    [filtered_correlated_coin_list.append(
        x) for x in correlated_coin_list if x not in filtered_correlated_coin_list]

    print(sorted(filtered_correlated_coin_list))


def get_all_correlated_values(history):
    verify_coins_files(history)

    coins_history = read_coins_history_file(history)
    ignored_coins = get_coins_from_file(
        ignored_coins_file) if os.path.isfile(ignored_coins_file) else []

    coin_list = []
    [coin_list.append(x) for x in get_coins_from_file(
        used_coins_file)[:first_n_coins] if x in coins_history and x not in ignored_coins]

    correlations = []
    sorted_correlations = {}

    combinations = get_all_coins_combinations(coin_list)

    for coins in combinations:
        correlations.append({"coin_a": coins[0], "coin_b": coins[1], "correlation": pearson_correlation(
            coins_history[coins[0]]['normalized'].tolist(), coins_history[coins[1]]['normalized'].tolist())})

    filtered_correlations = [
        c for c in correlations if c['correlation'] > correlation_greater_than and c['correlation'] <= correlation_less_than]
    sorted_correlations = sorted(
        filtered_correlations, key=lambda i: i['correlation'])

    for c in sorted_correlations:
        print(c['coin_a']+"/"+c['coin_b']+": "+str(round(c['correlation'], 2)))


def get_all_correlated_grouped(history):
    verify_coins_files(history)

    coins_history = read_coins_history_file(history)
    ignored_coins = get_coins_from_file(
        ignored_coins_file) if os.path.isfile(ignored_coins_file) else []
    coin_list = []
    [coin_list.append(x) for x in get_coins_from_file(
        used_coins_file)[:first_n_coins] if x in coins_history and x not in ignored_coins]

    correlations = []

    combinations = get_all_coins_combinations(coin_list)

    for coins in combinations:
        correlations.append({"coin_a": coins[0], "coin_b": coins[1], "correlation": pearson_correlation(
            coins_history[coins[0]]['normalized'].tolist(), coins_history[coins[1]]['normalized'].tolist())})

    filtered_correlations = [
        c for c in correlations if c['correlation'] > correlation_greater_than and c['correlation'] <= correlation_less_than]

    group_correlations(filtered_correlations)


def get_all_correlated_list(history):
    verify_coins_files(history)

    coins_history = read_coins_history_file(history)
    ignored_coins = get_coins_from_file(
        ignored_coins_file) if os.path.isfile(ignored_coins_file) else []
    coin_list = []
    [coin_list.append(x) for x in get_coins_from_file(
        used_coins_file)[:first_n_coins] if x in coins_history and x not in ignored_coins]

    correlations = []

    combinations = get_all_coins_combinations(coin_list)

    for coins in combinations:
        correlations.append({"coin_a": coins[0], "coin_b": coins[1], "correlation": pearson_correlation(
            coins_history[coins[0]]['normalized'].tolist(), coins_history[coins[1]]['normalized'].tolist())})

    filtered_correlations = [
        c for c in correlations if c['correlation'] > correlation_greater_than and c['correlation'] <= correlation_less_than]

    correlated_coin_list = []
    filtered_correlated_coin_list = []

    for c in filtered_correlations:
        correlated_coin_list.append(c['coin_a'])
        correlated_coin_list.append(c['coin_b'])

    #[filtered_correlated_coin_list.append(
    #    x) for x in correlated_coin_list if x not in filtered_correlated_coin_list]

    coin_by_volume = []
    if os.path.exists(used_coins_file):
        with open(used_coins_file) as rfh:
            for line in rfh:
                line = line.strip()
                if not line or line.startswith(
                        "#") or line in coin_by_volume:
                    continue
                coin_by_volume.append(line)

    with open(history) as json_file:
        valid_klines = json.load(json_file)


    new_coin_list = []
    #print(list(valid_klines))
    #print(list(filtered_correlated_coin_list))

    top_group = top_group_correlation(filtered_correlations)

    for coin in coin_by_volume:
        #if coin not in filtered_correlated_coin_list or coin not in list(valid_klines):
        if coin in sorted(top_group, key=len, reverse=True)[0] and coin in list(valid_klines):
            new_coin_list.append(coin)

    #print(sorted(filtered_correlated_coin_list))
    print("Updating supported_coin_list ...")
    try:
        with open('supported_coin_list', 'w') as writer:
            # save top 40 to file
            #for coin in new_coin_list[:40]:
            for coin in new_coin_list:
                    writer.write(coin+'\n')

        print("supported_coin_list updated successfully!")
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)

def group_correlations(correlations):
    l = [(c["coin_a"], c["coin_b"])
         for c in correlations]
    pool = set(map(frozenset, l))
    groups = []
    coin_groups = []
    while pool:
        group = set()
        groups.append([])
        while True:
            for candidate in pool:
                if not group or group & candidate:
                    group |= candidate
                    groups[-1].append(tuple(candidate))
                    pool.remove(candidate)
                    break
            else:
                break

    for g in groups:
        separated = []
        coin_list = []
        for c in g:
            separated.append(c[0])
            separated.append(c[1])
        for x in separated:
            if(x not in coin_list):
                coin_list.append(x)
        coin_groups.append(coin_list)

    for i in range(len(coin_groups)):
        print("Group "+str(i+1)+":")
        print(sorted(coin_groups[i]))

def top_group_correlation(correlations):
    l = [(c["coin_a"], c["coin_b"])
         for c in correlations]
    pool = set(map(frozenset, l))
    groups = []
    coin_groups = []
    while pool:
        group = set()
        groups.append([])
        while True:
            for candidate in pool:
                if not group or group & candidate:
                    group |= candidate
                    groups[-1].append(tuple(candidate))
                    pool.remove(candidate)
                    break
            else:
                break

    for g in groups:
        separated = []
        coin_list = []
        for c in g:
            separated.append(c[0])
            separated.append(c[1])
        for x in separated:
            if(x not in coin_list):
                coin_list.append(x)
        coin_groups.append(coin_list)

    return coin_groups

def verify_coins_files(history = coin_history_file, used = used_coins_file):
    if not os.path.isfile(history):
        raise Exception(
            "Coin history '"+history+"' not found, please run: binance_api.py --update-coins-history")

    if not os.path.isfile(used):
        raise Exception(
            "Top coins file '"+used+"' not found, please run: binance_api.py --update-top-coins")


def update_coin_historical_klines(history = coin_history_file):

    coins_history = {}
    requested_start = history_start.replace(minute=0, second=0, microsecond=0).timestamp()
    requested_end = history_end.replace(minute=0, second=0, microsecond=0).timestamp()
    print(f'Fetching trade data between "{history_start} ({requested_start}) and {history_end} ({requested_end})')

    try:
        with open(history) as json_file:
            klines = json.load(json_file)
    except:
        klines = None

    count = 0
    all_tickers_current = get_all_tickers(paired_coin)
    for coin in all_tickers_current:
        
        #print("Getting "+coin+paired_coin+" history data... " + str(round((count*100)/len(all_tickers_current))) + "%")
        
        if klines is None or os.stat(history).st_size == 0 or coin not in list(klines) :
            coins_history.update(get_coin_history(coin, paired_coin))
        else:
            try:
                df = pd.DataFrame.from_records(klines[coin], columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                   'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])

                saved_start = datetime.fromtimestamp(int(df['open_time'].values[0]) / 1000, tz=timezone.utc).replace(minute=0, second=0, microsecond=0).timestamp()
                saved_end = datetime.fromtimestamp(int(df['close_time'].values[-1]) / 1000, tz=timezone.utc).replace(minute=0, second=0, microsecond=0).timestamp()

                if (requested_start == saved_start) and (requested_end == saved_end):
                    coins_history[coin] = klines[coin]
                else:
                    print(f"Invalid dates for {coin} - updating klines ...")
                    coins_history.update(get_coin_history(coin, paired_coin))
            except Exception as e:
                print(f"Unable to update history with saved data for {coin} - {e}")
                coins_history.update(get_coin_history(coin, paired_coin))

        # Keep klines with data for full date range
        if len(coins_history[coin]) != 0:
            hindsight = (len(coins_history[coin]) / 1440)
        else:
            hindsight = 0

        if hindsight <= history_delta:
            print(f"Removing {coin} from list - Not enough hindsight ({math.ceil(hindsight)}/{history_delta})")
            del coins_history[coin]

        count = count + 1

    # create folder structure
    if not os.path.exists(os.path.dirname(os.path.abspath(history))):
        os.makedirs(os.path.dirname(os.path.abspath(history)))

    with open(history, 'w') as outfile:
        json.dump(coins_history, outfile)

def read_coins_history_file(history = coin_history_file):
    kline_df = {}
    data = {}

    with open(history) as json_file:
        data = json.load(json_file)

    for coin in data:
        if(len(data[coin]) > 0):
            kline_df[coin] = klines_to_df(data[coin])

    return kline_df


def update_top_ranked_coins():
    headers = {
        'Accepts': 'application/json',
    }

    ignored_coins = []

    # get stablecoin list
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=stablecoins&order=market_cap_desc&per_page=250&page=1&sparkline=false'
    session = Session()
    session.headers.update(headers)
    response = session.get(url)
    raw_list = json.loads(response.text)
    for coin in raw_list:
      ignored_coins.append(coin['symbol'].upper())

    # get compond token list
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=compound-tokens&order=market_cap_desc&per_page=250&page=1&sparkline=false'
    session = Session()
    session.headers.update(headers)
    response = session.get(url)
    raw_list = json.loads(response.text)
    for coin in raw_list:
      ignored_coins.append(coin['symbol'].upper())

    # get compond token list
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=aave-tokens&order=market_cap_desc&per_page=250&page=1&sparkline=false'
    session = Session()
    session.headers.update(headers)
    response = session.get(url)
    raw_list = json.loads(response.text)
    for coin in raw_list:
      ignored_coins.append(coin['symbol'].upper())

    # get wrapped token list
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=wrapped-tokens&order=market_cap_desc&per_page=250&page=1&sparkline=false'
    session = Session()
    session.headers.update(headers)
    response = session.get(url)
    raw_list = json.loads(response.text)
    for coin in raw_list:
      ignored_coins.append(coin['symbol'].upper())

    # get eth 2.0 staking token list
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=eth-2-0-staking&order=market_cap_desc&per_page=250&page=1&sparkline=false'
    session = Session()
    session.headers.update(headers)
    response = session.get(url)
    raw_list = json.loads(response.text)
    for coin in raw_list:
      ignored_coins.append(coin['symbol'].upper())

    # get top 250 coins
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1&sparkline=false'
    session = Session()
    session.headers.update(headers)
    response = session.get(url)
    data = json.loads(response.text)

    fullList = {}

    targetDate = history_end.strftime('%d-%m-%Y')
    
    print("Fetching trade volume data for " + history_end.replace(tzinfo=timezone.utc).astimezone(tz=None).strftime('%d %B %Y'))
    for coin in data:
        if any([x in coin['symbol'].upper() for x in ['BULL', 'BEAR','UP', 'DOWN', 'HEDGE', 'LONG', 'SHORT']]) or coin['symbol'].upper() in ignored_coins:
            data.remove(coin)
            continue
        
        dirName = "temp/" + str(targetDate)
        try:
            with open(dirName + '/coinVolume.json') as json_file:
                coinVolume = json.load(json_file)
        except:
            coinVolume = None

        if coinVolume is None or os.stat(dirName + '/coinVolume.json').st_size == 0 or coin['symbol'].upper() not in coinVolume:
          url = 'https://api.coingecko.com/api/v3/coins/' + str(coin['id']) + "/history?date=" + str(targetDate) + "&localization=false"

          session = Session()
          session.headers.update(headers)

          response = session.get(url)
          history = json.loads(response.text)

          try:
              #print(str(history['symbol']).upper() + ' ## ' + str(history['market_data']['total_volume']['usd']))
              fullList[history['symbol'].upper()] = float(history['market_data']['total_volume']['usd'])
          except:
              #print(str(history['symbol']).upper() + ' ## unavailable!' )
              continue

          time.sleep(1.5)

        else:
          fullList[coin['symbol'].upper()] = coinVolume[coin['symbol'].upper()]

    # create folder structure
    if not os.path.exists(dirName):
        os.makedirs(dirName)

    with open(dirName + '/coinVolume.json', 'w') as outfile:
        json.dump(fullList, outfile)

    print("Parsing top "+str(top_n_ranked_coins)+" correlated coins...")
    try:
        with open(used_coins_file, 'w') as writer:
            # Sort shortList by value
            for coin in sorted(fullList, key=fullList.get, reverse=True)[:top_n_ranked_coins]:
                if float(fullList[coin]) > 0:
                    writer.write(coin+'\n')
                    #print(f"{coin} -> {fullList[coin]}")
                    
        print("Top coin list stored successfully!")
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)

def main(args):

    global first_n_coins, top_n_ranked_coins, correlation_greater_than, correlation_less_than, paired_coin, history_start, history_delta, history_end, history_interval, coin_history_file, used_coins_file, ignored_coins_file

    # read optional args
    if "start_datetime" in args and args["start_datetime"]:
      try:
        #history_start = datetime.strptime(args["start_datetime"][0], '%Y-%m-%d.%H:%M:%S').replace(tzinfo=timezone.utc).astimezone(tz=None)
        history_start = datetime.strptime(args["start_datetime"][0], '%Y-%m-%d.%H:%M:%S').astimezone(tz=timezone.utc)
      except:
        print('Invalid Date format - expected : "%Y-%m-%d.%H:%M:%S"')
        exit()

    if "end_datetime" in args and args["end_datetime"]:
      try:
        #history_end = datetime.strptime(args["end_datetime"][0], '%Y-%m-%d.%H:%M:%S').replace(tzinfo=timezone.utc).astimezone(tz=None)
        history_end = datetime.strptime(args["end_datetime"][0], '%Y-%m-%d.%H:%M:%S').astimezone(tz=timezone.utc)
      except:
        print('Invalid Date format - expected : "%Y-%m-%d.%H:%M:%S"')
        exit()

    if "date_offset" in args and args["date_offset"] and int(args["date_offset"][0]) > 0:
      try:
        history_delta = int(args["date_offset"][0])
      except:
        print('Offset must be positive - expected : INT > 0')
        exit()

    if "paired_coin" in args and args["paired_coin"]:
      try:
        paired_coin = str(args["paired_coin"][0])
      except:
        pass

    if history_start is None:
      history_start = (history_end - timedelta(days = history_delta))
      
    if "update_top_coins" in args and args["update_top_coins"]:
      update_top_ranked_coins()
    
    if "update_coins_history" in args and args["update_coins_history"]:
      update_coin_historical_klines("temp/" + str(history_end.strftime('%d-%m-%Y')) + "/klines.json")

    if "all_correlated_values" in args and args["all_correlated_values"]:
        get_all_correlated_values("temp/" + str(history_end.strftime('%d-%m-%Y')) + "/klines.json")
    
    if "one_correlated_values" in args and args["one_correlated_values"]:
        get_one_correlated_values(args["one_correlated_values"][0], "temp/" + str(history_end.strftime('%d-%m-%Y')) + "/klines.json")
    
    if "all_correlated_list" in args and args["all_correlated_list"]:
        get_all_correlated_list("temp/" + str(history_end.strftime('%d-%m-%Y')) + "/klines.json")
    
    if "one_correlated_list" in args and args["one_correlated_list"]:
        get_one_correlated_list(args["one_correlated_list"][0], "temp/" + str(history_end.strftime('%d-%m-%Y')) + "/klines.json")
    
    if "all_correlated_grouped" in args and args["all_correlated_grouped"]:
        get_all_correlated_grouped("temp/" + str(history_end.strftime('%d-%m-%Y')) + "/klines.json")