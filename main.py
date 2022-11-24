# https://www.reddit.com/r/options/comments/qw3pzt/ultimate_guide_to_selling_options_profitably_part/
# https://classic-desert-4cb.notion.site/Ultimate-Guide-to-Selling-Options-061ca90e28cc494eb855de5ce398af7e


import numpy as np
#import tensorflow
import pandas as pd
import pandas_datareader as pdr
from pandas_datareader import data as pdrd
import yfinance as yf
import matplotlib.pyplot as plt
import pickle as pickle
import os
from timeit import default_timer as timer
from datetime import datetime
import csv

class HistoricalData:
    #symbol is ticker symbol
    #days_covered is a list of dates in order
    #data is the dataframe
    def __init__(self, symbol, days_covered, data):
        self.symbol = symbol
        self.days_covered = days_covered
        self.data = data

class Option:
    #symbol is the ticker symbol, string
    #exp_date is the expiration date, string
    #strike is the strike price, float
    #is_call is the type of option, boolean
    def __init__(self, symbol, exp_date, strike, is_call):
        self.symbol = symbol
        self.exp_date = exp_date
        self.strike = strike
        self.is_call = is_call

def save_object(obj, filename):
    with open(filename, 'wb') as outp:  # Overwrites any existing file.
        pickle.dump(obj, outp, pickle.HIGHEST_PROTOCOL)

########################## pickle save ticker data ###########################
def saveTickerData(ticker, start, end):
    data = yf.download(ticker, start, end)
    data.to_pickle("./" + ticker + ".pkl")
    print(data)

def readTickerData(ticker):
    unpickled_df = pd.read_pickle("./" + ticker + ".pkl")
    return unpickled_df


def saveOptionData(ticker):
    #based on folder structure of the option data
    #second level XXXX_XG
    #third level XXXXMarch
    single_dataset_years = ['2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012']
    double_dataset_years = ['2013', '2014', '2015', '2016', '2017', '2018']
    three_dataset_years = ['2019', '2020', '2021']
    months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

    days_list = []
    dataframes = []

    base_path = "../../historical_option_data/"

    for i in single_dataset_years:
        path = base_path
        path += i
        path += "/"
        for j in months:
            path2 = path + i
            path2 += j
            files = sorted(os.listdir(path2))
            path2 += "/"
            for k in files:
                path3 = path2 + k
                raw_date = k.split("_")[1]
                date = raw_date[0:4] + "-" + raw_date[4:6] + "-" + raw_date[6:8]
                days_list.append(date)
                dataframes.append(readCsv(ticker, path3))
                #print("file read")

    for i in double_dataset_years:
        path = base_path
        path += i
        path += "/"
        for j in range(6):
            path2 = path + i + "_1G/" + i
            path2 += months[j]
            files = sorted(os.listdir(path2))
            path2 += "/"
            for k in files:
                path3 = path2 +  k
                raw_date = k.split("_")[1]
                date = raw_date[0:4] + "-" + raw_date[4:6] + "-" + raw_date[6:8]
                days_list.append(date)
                dataframes.append(readCsv(ticker, path3))

        for j in range(6, 12):
            path2 = path + i + "_2G/" + i
            path2 += months[j]
            files = sorted(os.listdir(path2))
            path2 += "/"
            for k in files:
                path3 = path2 + k
                raw_date = k.split("_")[1]
                date = raw_date[0:4] + "-" + raw_date[4:6] + "-" + raw_date[6:8]
                days_list.append(date)
                dataframes.append(readCsv(ticker, path3))

    for i in three_dataset_years:
        path = base_path
        path += i
        path += "/"
        for j in range(4):
            path2 = path + i + "_1G/" + i
            path2 += months[j]
            files = sorted(os.listdir(path2))
            path2 += "/"
            for k in files:
                path3 = path2 +  k
                raw_date = k.split("_")[1]
                date = raw_date[0:4] + "-" + raw_date[4:6] + "-" + raw_date[6:8]
                days_list.append(date)
                dataframes.append(readCsv(ticker, path3))

        for j in range(4, 8):
            path2 = path + i + "_2G/" + i
            path2 += months[j]
            files = sorted(os.listdir(path2))
            path2 += "/"
            for k in files:
                path3 = path2 + k
                raw_date = k.split("_")[1]
                date = raw_date[0:4] + "-" + raw_date[4:6] + "-" + raw_date[6:8]
                days_list.append(date)
                dataframes.append(readCsv(ticker, path3))

        for j in range(8, 12):
            path2 = path + i + "_3G/" + i
            path2 += months[j]
            files = sorted(os.listdir(path2))
            path2 += "/"
            for k in files:
                path3 = path2 + k
                raw_date = k.split("_")[1]
                date = raw_date[0:4] + "-" + raw_date[4:6] + "-" + raw_date[6:8]
                days_list.append(date)
                dataframes.append(readCsv(ticker, path3))

    total_data = pd.concat(dataframes)
    data_obj = HistoricalData(ticker, days_list[:], total_data)
    return data_obj


def readCsv(ticker, path):
    #data = pd.read_csv(path, skiprows= lambda x: isinstance(data.iloc[[x]]['ImpliedVolatility'], str), dtype={'OpenInterest': int, 'UnderlyingPrice': float, 'Volume': int, 'ImpliedVolatility': float, 'StrikePrice': float})
    data = pd.read_csv(path)
    #ret = data[data['ImpliedVolatility'].apply(lambda x: not isinstance(x, str))]
    return data[data["Symbol"] == 'SPY']

########################## average daily range ###############################
def avg_daily_range(ticker, start, end):
    data = yf.download(ticker, start, end)
    vix_data = yf.download('^VIX', start, end)

    total_range = 0
    count = 0

    range_vix_under_20 = 0
    count_vix_under_20 = 0

    range_vix_under_30 = 0
    count_vix_under_30 = 0

    range_vix_under_40 = 0
    count_vix_under_40 = 0

    range_vix_over_40 = 0
    count_vix_over_40 = 0

    for ind in data.index:
        daily_range = data['High'][ind] - data['Low'][ind]
        total_range += daily_range / data['Open'][ind]
        count += 1
        if vix_data['Open'][ind] < 20:
            range_vix_under_20 += daily_range / data['Open'][ind]
            count_vix_under_20 += 1
        elif vix_data['Open'][ind] >= 20 and vix_data['Open'][ind] < 30:
            range_vix_under_30 += daily_range / data['Open'][ind]
            count_vix_under_30 += 1
        elif vix_data['Open'][ind] >= 30 and vix_data['Open'][ind] < 40:
            range_vix_under_40 += daily_range / data['Open'][ind]
            count_vix_under_40 += 1
        elif vix_data['Open'][ind] >= 40:
            range_vix_over_40 += daily_range / data['Open'][ind]
            count_vix_over_40 += 1

    print("Avg daily range: ", (total_range / len(data.index)) * 100, "%")
    print("Avg daily range (VIX under 20): ", (range_vix_under_20 / count_vix_under_20) * 100, "%")
    print("Avg daily range (VIX under 30): ", (range_vix_under_30 / count_vix_under_30) * 100, "%")
    print("Avg daily range (VIX under 40): ", (range_vix_under_40 / count_vix_under_40) * 100, "%")
    print("Avg daily range (VIX over 40): ", (range_vix_over_40 / count_vix_over_40) * 100, "%")

########################## average weekly range ###############################
def avg_weekly_range(ticker, start, end):
    weekly_data = yf.download(ticker, interval = "1wk", start=start, end=end)
    weekly_vix_data = yf.download('^VIX', interval = "1wk", start=start, end=end)

    weekly_total_range = 0
    weekly_count = 0

    weekly_range_vix_under_20 = 0
    weekly_count_vix_under_20 = 0

    weekly_range_vix_under_30 = 0
    weekly_count_vix_under_30 = 0

    weekly_range_vix_under_40 = 0
    weekly_count_vix_under_40 = 0

    weekly_range_vix_over_40 = 0
    weekly_count_vix_over_40 = 0

    for ind in weekly_data.index:
        weekly_range = weekly_data['High'][ind] - weekly_data['Low'][ind]
        weekly_total_range += weekly_range / weekly_data['Open'][ind]
        weekly_count += 1
        if weekly_vix_data['Open'][ind] < 20:
            weekly_range_vix_under_20 += weekly_range / weekly_data['Open'][ind]
            weekly_count_vix_under_20 += 1
        elif weekly_vix_data['Open'][ind] >= 20 and weekly_vix_data['Open'][ind] < 30:
            weekly_range_vix_under_30 += weekly_range / weekly_data['Open'][ind]
            weekly_count_vix_under_30 += 1
        elif weekly_vix_data['Open'][ind] >= 30 and weekly_vix_data['Open'][ind] < 40:
            weekly_range_vix_under_40 += weekly_range / weekly_data['Open'][ind]
            weekly_count_vix_under_40 += 1
        elif weekly_vix_data['Open'][ind] >= 40:
            weekly_range_vix_over_40 += weekly_range / weekly_data['Open'][ind]
            weekly_count_vix_over_40 += 1

    print("Avg weekly range: ", (weekly_total_range / len(weekly_data.index)) * 100, "%")
    print("Avg weekly range (VIX under 20): ", (weekly_range_vix_under_20 / weekly_count_vix_under_20) * 100, "%")
    print("Avg weekly range (VIX under 30): ", (weekly_range_vix_under_30 / weekly_count_vix_under_30) * 100, "%")
    print("Avg weekly range (VIX under 40): ", (weekly_range_vix_under_40 / weekly_count_vix_under_40) * 100, "%")
    print("Avg weekly range (VIX over 40): ", (weekly_range_vix_over_40 / weekly_count_vix_over_40) * 100, "%")

def load_days_from_pkl(path):
    file = open(path, 'rb')
    data = pickle.load(file)
    file.close()
    return data



############### store new symbol for backtests ########################
def store_new_symbol(ticker):
    obj = saveOptionData(ticker)
    print(obj.symbol)
    print(obj.days_covered)
    print(obj.data)

    obj.data.to_pickle("./stored_data/" + ticker + "_options_data.pkl")
    with open("./stored_data/" + ticker + "_options_days.pkl", 'wb') as f:
        pickle.dump(obj.days_covered, f)

############# load existing data for a symbol ####################
############# returns historical data object #####################
def load_historical_data(ticker):
    data = pd.read_pickle("./stored_data/" + ticker + "_options_data.pkl")
    days = load_days_from_pkl("./stored_data/spy_options_days.pkl")
    data_obj = HistoricalData(ticker, days, data)
    return data_obj

def backtest_pmcc1_spy():
    start = timer()
    spy_prices = yf.download('SPY', '2005-01-03', '2021-12-31')
    vix_prices = yf.download('^VIX', '2005-01-03', '2021-12-31')
    data_obj = load_historical_data('SPY')
    end = timer()
    print(end - start) # Time in seconds, e.g. 5.38091952400282

    trading_days = data_obj.data['DataDate'].unique()

    start = timer()

    spy_prices_df = pd.DataFrame(spy_prices, columns=['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])

    long_option = None
    short_option = None
    last_long_price = None
    last_short_price = None

    cash_balance = 100000
    balance_invested = 0

    num_long_buys = 0
    num_long_closes = 0
    num_short_sells = 0
    num_short_closes = 0

    account_value_per_day = []
    result_data = []

    for day in trading_days:
        ############## skipping some missing data for spy #####################
        #if '2005-01-21' > day:
        if '2007-10-18' > day and '/' not in day:
            #print("skipping: ", day)
            continue
        elif '2005-01-21' == day:
            print("done skipping")

        options_chain = data_obj.data.loc[data_obj.data['DataDate'] == day]

        ##### if we already have a position
        if short_option is not None and long_option is not None:
            print("one")
            #track portfolio value
            long_call = options_chain.loc[(options_chain['ExpirationDate'] == long_option.exp_date) & (options_chain['PutCall'] == "call") & (abs(options_chain['StrikePrice']) == long_option.strike)]
            short_call = options_chain.loc[(options_chain['ExpirationDate'] == short_option.exp_date) & (options_chain['PutCall'] == "call") & (abs(options_chain['StrikePrice']) == short_option.strike)]

            #if we can't find our strike anymore close everything and reopen #####################################################
            current_position_not_found = False
            if long_call.empty or short_call.empty:
                 current_position_not_found = True
            #if there is a bad mark for a long call just close it to avoid issues with the data
            bad_mark = False
            if not long_call.empty:
                if long_call['BidPrice'].iloc[0] < (long_call['UnderlyingPrice'].iloc[0] - long_call['StrikePrice'].iloc[0]):
                    print("closing due to bad mark")
                    bad_mark = True

            if current_position_not_found or bad_mark:
                long_option = None
                short_option = None
                if not long_call.empty:
                    if bad_mark:
                        cash_balance += last_long_price * 100
                        balance_invested -= last_long_price * 100
                    else:
                        cash_balance += long_call['BidPrice'].iloc[0] * 100
                        balance_invested -= long_call['BidPrice'].iloc[0] * 100
                else:
                    cash_balance += last_long_price * 100
                    balance_invested -= last_long_price * 100
                last_long_price = None
                num_long_closes += 1
                if not short_call.empty:
                    cash_balance -= short_call['AskPrice'].iloc[0] * 100
                    balance_invested += short_call['AskPrice'].iloc[0] * 100
                else:
                    cash_balance -= last_short_price * 100
                    balance_invested += last_short_price * 100
                last_short_price = None
                num_short_closes += 1
                print("Couldn't find the current position anymore. Closing all positions")
            else:
                print("two")
                #account_value += ((long_call['BidPrice'].iloc[0] - last_long_price) * 100)
                balance_invested = (long_call['BidPrice'].iloc[0] * 100) - (short_call['AskPrice'].iloc[0] * 100)
                last_long_price = long_call['BidPrice'].iloc[0]
                last_short_price = short_call['AskPrice'].iloc[0]

                ###################### remove later ############################3
                #print("Testing: ")
                #print(short_call)
                ################################################################333

                #account_value -= ((short_call['AskPrice'].iloc[0] - last_short_price) * 100)
                last_long_price = long_call['BidPrice'].iloc[0]
                last_short_price = short_call['AskPrice'].iloc[0]

                exps = options_chain['ExpirationDate'].unique()

                short_call_itm = False
                current_dt = datetime.strptime(day, '%Y-%m-%d').date() if ('/' not in day) else datetime.strptime(day, '%m/%d/%Y').date()
                current_exp_date = datetime.strptime(short_option.exp_date, '%Y-%m-%d').date() if ('/' not in short_option.exp_date) else datetime.strptime(short_option.exp_date, '%m/%d/%Y').date()
                #if the short call expires today roll it
                if day == short_option.exp_date or (current_dt.weekday() == 4 and (current_exp_date - current_dt).days == 1):
                    #if in the money, close entire position
                    if short_call['StrikePrice'].iloc[0] < short_call['UnderlyingPrice'].iloc[0]:
                        print("short call itm, strike: ", short_call['StrikePrice'].iloc[0], " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                        short_call_itm = True

                    #close short call
                    cash_balance -= short_call['AskPrice'].iloc[0] * 100
                    balance_invested += short_call['AskPrice'].iloc[0] * 100
                    print("CLOSING SHORT CALLS: ", short_call['AskPrice'].iloc[0])
                    short_option = None
                    num_short_closes += 1

                    ############### dates switch formats in 2019 ##################################
                    current_dt = datetime.strptime(day, '%Y-%m-%d').date() if ('/' not in day) else datetime.strptime(day, '%m/%d/%Y').date()
                    short_exp_date = None
                    for date in exps:
                        ############### dates switch formats in 2019 ##################################
                        exp_date = datetime.strptime(date, '%Y-%m-%d').date() if ('/' not in date) else datetime.strptime(date, '%m/%d/%Y').date()
                        if (exp_date - current_dt).days >= 6:
                            short_exp_date = date
                            break

                    if short_exp_date is not None:
                        short_calls = options_chain.loc[(options_chain['ExpirationDate'] == short_exp_date) & (options_chain['PutCall'] == "call") & (abs(options_chain['Delta']) < .5)].drop_duplicates()
                        call_index = 0
                        delta_diff = 1000
                        for call_ind in short_calls.index:
                            if isinstance(short_calls['Delta'][call_ind], (int, float)):
                                abs_diff = abs(abs(short_calls['Delta'][call_ind]) - .16)
                                if abs_diff < delta_diff:
                                    call_index = call_ind
                                    delta_diff = abs_diff
                        if short_calls.empty:
                            continue
                        #sell order
                        if short_calls['BidPrice'][call_index] > 0:
                            print("short call exp: ", short_calls['ExpirationDate'][call_index], ", Delta: ", short_calls['Delta'][call_index], " strike: ", short_calls['StrikePrice'][call_index], " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                            short_option = Option('SPY', short_calls['ExpirationDate'][call_index], short_calls['StrikePrice'][call_index], True)
                            last_short_price = short_calls['BidPrice'][call_index]
                            print("short call premium: ", short_calls['BidPrice'][call_index])
                            cash_balance += short_calls['BidPrice'][call_index] * 100
                            balance_invested -= short_call['BidPrice'].iloc[0] * 100
                            num_short_sells += 1


                #if there's a new expirtion or the short call is expiring itm roll the long call
                if exps[-1] > long_option.exp_date or short_call_itm:

                    #close long position
                    cash_balance += long_call['BidPrice'].iloc[0] * 100
                    balance_invested -= long_call['BidPrice'].iloc[0] * 100
                    num_long_closes += 1
                    print("rolling long call")

                    long_calls = options_chain.loc[(options_chain['ExpirationDate'] == exps[-1]) & (options_chain['PutCall'] == "call") & (abs(options_chain['Delta']) > .5)].drop_duplicates()
                    call_index = 0
                    delta_diff = 1000
                    for call_ind in long_calls.index:
                        if isinstance(long_calls['Delta'][call_ind], (int, float)):
                            abs_diff = abs(abs(long_calls['Delta'][call_ind]) - .9)
                            if abs_diff < delta_diff:
                                call_index = call_ind
                                delta_diff = abs_diff
                    if long_calls.empty:
                        continue
                    if long_calls['BidPrice'][call_ind] < (long_calls['UnderlyingPrice'][call_ind] - long_calls['StrikePrice'][call_ind]):
                        print("skipped bad mark")
                        continue

                    #buy order
                    print("Long call exp: ", exps[-1], ", Delta: ", long_calls['Delta'][call_index], " strike: ", long_calls['StrikePrice'][call_index], " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                    long_option = Option('SPY', long_calls['ExpirationDate'][call_index], long_calls['StrikePrice'][call_index], True)
                    last_long_price = long_calls['AskPrice'][call_index]
                    cash_balance -= long_call['AskPrice'].iloc[0] * 100
                    balance_invested += long_call['AskPrice'].iloc[0] * 100
                    num_long_buys += 1

        if long_option is None:
            print("three")
            exps = options_chain['ExpirationDate'].unique()
            long_calls = options_chain.loc[(options_chain['ExpirationDate'] == exps[-1]) & (options_chain['PutCall'] == "call") & (abs(options_chain['Delta']) > .5)].drop_duplicates()
            call_index = 0
            delta_diff = 1000
            for call_ind in long_calls.index:
                if isinstance(long_calls['Delta'][call_ind], (int, float)):
                    abs_diff = abs(abs(long_calls['Delta'][call_ind]) - .9)
                    if abs_diff < delta_diff:
                        call_index = call_ind
                        delta_diff = abs_diff
            if long_calls.empty:
                continue
            if long_calls['BidPrice'][call_ind] < (long_calls['UnderlyingPrice'][call_ind] - long_calls['StrikePrice'][call_ind]):
                print("skipped bad mark 2")
                continue

            #buy order
            print("Long call exp: ", exps[-1], ", Delta: ", long_calls['Delta'][call_index], " strike: ", long_calls['StrikePrice'][call_index], " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
            long_option = Option('SPY', long_calls['ExpirationDate'][call_index], long_calls['StrikePrice'][call_index], True)
            last_long_price = long_calls['AskPrice'][call_index]
            cash_balance -= long_calls['AskPrice'][call_index] * 100
            balance_invested += long_calls['AskPrice'][call_index] * 100
            num_long_buys += 1

        if short_option is None:
            print("four")
            exps = options_chain['ExpirationDate'].unique()

            ############### dates switch formats in 2019 ##################################
            current_dt = datetime.strptime(day, '%Y-%m-%d').date() if ('/' not in day) else datetime.strptime(day, '%m/%d/%Y').date()
            short_exp_date = None
            for date in exps:
                ############### dates switch formats in 2019 ##################################
                exp_date = datetime.strptime(date, '%Y-%m-%d').date() if ('/' not in date) else datetime.strptime(date, '%m/%d/%Y').date()
                if (exp_date - current_dt).days >= 6:
                    short_exp_date = date
                    break

            if short_exp_date is not None:
                short_calls = options_chain.loc[(options_chain['ExpirationDate'] == short_exp_date) & (options_chain['PutCall'] == "call") & (abs(options_chain['Delta']) < .5)].drop_duplicates()
                call_index = 0
                delta_diff = 1000
                for call_ind in short_calls.index:
                    if isinstance(short_calls['Delta'][call_ind], (int, float)):
                        abs_diff = abs(abs(short_calls['Delta'][call_ind]) - .16)
                        if abs_diff < delta_diff:
                            call_index = call_ind
                            delta_diff = abs_diff
                if short_calls.empty:
                    continue
                #sell order
                if short_calls['BidPrice'][call_index] > 0:
                    print("Short call exp: ", short_calls['ExpirationDate'][call_index], ", Delta: ", short_calls['Delta'][call_index], " strike: ", short_calls['StrikePrice'][call_index], " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                    short_option = Option('SPY', short_calls['ExpirationDate'][call_index], short_calls['StrikePrice'][call_index], True)
                    last_short_price = short_calls['BidPrice'][call_index]
                    print("short call premium: ", short_calls['BidPrice'][call_index])
                    cash_balance += short_calls['BidPrice'][call_index] * 100
                    balance_invested -= short_calls['BidPrice'][call_index] * 100
                    num_short_sells += 1

        account_value_per_day.append(cash_balance + balance_invested)
        result_data.append({'Date': day, 'AccountValue': (cash_balance + balance_invested)})
        print(day, " cash: $", cash_balance, " invested: $", balance_invested, " total value: $", (cash_balance + balance_invested), " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")

    print("five")
    keys = ['Date', 'AccountValue']

    with open('spy_backtest_results.csv', 'a') as output_file:
        dict_writer = csv.DictWriter(output_file, restval="-", fieldnames=keys, delimiter=',')
        dict_writer.writeheader()
        dict_writer.writerows(result_data)

    end = timer()
    print(end - start) # Time in seconds, e.g. 5.38091952400282

    print("number of short sells: ", num_short_sells)
    print("number of shorts bought back: ", num_short_closes)
    print("number of long buys: ", num_long_buys)
    print("number of long sold again ", num_long_closes)

#plots results from backtest 1
def plot_results():
    df = pd.read_csv('spy_backtest_results.csv')
    df = df.set_index('Date')
    spy_prices = yf.download('SPY', '2005-01-03', '2021-12-31')
    print(df)
    print(spy_prices)

    spy_prices_match = []
    dates = []
    datetimes = []

    for date in spy_prices.index:
        if str(date.date()) in df.index:
            spy_prices_match.append(spy_prices['Close'][date])
            dates.append(str(date.date()))
            datetimes.append(date.date())

    #normalize spy prices for the size of the portfolio
    factor = 100000 / spy_prices['Close'][dates[0]]
    print(factor)
    for i in range(len(spy_prices_match)):
        spy_prices_match[i] = spy_prices_match[i] * factor

    account_vals = df['AccountValue'][df.index.isin(dates)].tolist()
    for i in range(len(account_vals)):
        account_vals[i] = 100000 + ((account_vals[i] - 100000) * 5)

    print("lowest point: ", min(account_vals))

    plt.plot(datetimes, spy_prices_match, label="SPY")
    #plt.plot(datetimes, df['AccountValue'][df.index.isin(dates)], label="Backtest")
    plt.plot(datetimes, account_vals, label="Backtest")
    plt.show()

#backtest 2
def backtest_pmcc1_spy2():
    start = timer()
    spy_prices = yf.download('SPY', '2005-01-03', '2021-12-31')
    vix_prices = yf.download('^VIX', '2005-01-03', '2021-12-31')
    data_obj = load_historical_data('SPY')
    end = timer()
    print(end - start) # Time in seconds, e.g. 5.38091952400282

    trading_days = data_obj.data['DataDate'].unique()

    start = timer()

    spy_prices_df = pd.DataFrame(spy_prices, columns=['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])
    print(spy_prices_df)

    long_option = None
    short_option = None
    last_long_price = None
    last_short_price = None

    cash_balance = 100000
    balance_invested = 0
    premium_collected = 0

    num_long_buys = 0
    num_long_closes = 0
    num_short_sells = 0
    num_short_closes = 0

    cost_basis = None

    account_value_per_day = []
    result_data = []

    for day in trading_days:
        ############## skipping some missing data for spy #####################
        #if '2005-01-21' > day:
        #if '2012-01-27' > day and '/' not in day:
        if '2007-10-18' > day and '/' not in day:
            #print("skipping: ", day)
            continue
        elif '2005-01-21' == day:
            print("done skipping")

        print("one")
        if cost_basis is not None:
            print("cost basis: ", cost_basis)

        options_chain = data_obj.data.loc[data_obj.data['DataDate'] == day]

        ##### if we already have a position
        if short_option is not None and long_option is not None:
            print("two")
            #track portfolio value
            #existing positions
            long_call = options_chain.loc[(options_chain['ExpirationDate'] == long_option.exp_date) & (options_chain['PutCall'] == "call") & (abs(options_chain['StrikePrice']) == long_option.strike)]
            short_call = options_chain.loc[(options_chain['ExpirationDate'] == short_option.exp_date) & (options_chain['PutCall'] == "call") & (abs(options_chain['StrikePrice']) == short_option.strike)]

            #if we can't find our strike anymore close everything and reopen #####################################################
            current_position_not_found = False
            if long_call.empty or short_call.empty:
                 current_position_not_found = True
            #if there is a bad mark for a long call just close it to avoid issues with the data
            bad_mark = False
            if not long_call.empty:
                if long_call['BidPrice'].iloc[0] < (long_call['UnderlyingPrice'].iloc[0] - long_call['StrikePrice'].iloc[0]):
                    print("closing due to bad mark")
                    bad_mark = True

            if current_position_not_found or bad_mark:
                long_option = None
                short_option = None
                if not long_call.empty:
                    if bad_mark:
                        cash_balance += last_long_price * 100
                        balance_invested -= last_long_price * 100
                    else:
                        cash_balance += long_call['BidPrice'].iloc[0] * 100
                        balance_invested -= long_call['BidPrice'].iloc[0] * 100
                else:
                    cash_balance += last_long_price * 100
                    balance_invested -= last_long_price * 100
                last_long_price = None
                #cost_basis = None
                num_long_closes += 1
                if not short_call.empty:
                    cash_balance -= short_call['AskPrice'].iloc[0] * 100
                    balance_invested += short_call['AskPrice'].iloc[0] * 100
                    premium_collected -= short_call['AskPrice'].iloc[0] * 100
                else:
                    cash_balance -= last_short_price * 100
                    balance_invested += last_short_price * 100
                    premium_collected -= last_short_price * 100
                last_short_price = None
                num_short_closes += 1
                print("Couldn't find the current position anymore. Closing all positions")
            else:
                print("three")
                #account_value += ((long_call['BidPrice'].iloc[0] - last_long_price) * 100)
                balance_invested = (long_call['BidPrice'].iloc[0] * 100) - (short_call['AskPrice'].iloc[0] * 100)
                last_long_price = long_call['BidPrice'].iloc[0]
                last_short_price = short_call['AskPrice'].iloc[0]

                ###################### remove later ############################3
                #print("Testing: ")
                #print(short_call)
                ################################################################333

                #account_value -= ((short_call['AskPrice'].iloc[0] - last_short_price) * 100)
                last_long_price = long_call['BidPrice'].iloc[0]
                last_short_price = short_call['AskPrice'].iloc[0]

                exps = options_chain['ExpirationDate'].unique()

                short_call_itm = False
                current_dt = datetime.strptime(day, '%Y-%m-%d').date() if ('/' not in day) else datetime.strptime(day, '%m/%d/%Y').date()
                current_exp_date = datetime.strptime(short_option.exp_date, '%Y-%m-%d').date() if ('/' not in short_option.exp_date) else datetime.strptime(short_option.exp_date, '%m/%d/%Y').date()
                #if the short call expires today roll it
                if day == short_option.exp_date or (current_dt.weekday() == 4 and (current_exp_date - current_dt).days == 1):
                    #if in the money, close entire position
                    if short_call['StrikePrice'].iloc[0] < short_call['UnderlyingPrice'].iloc[0]:
                        print("short call itm, strike: ", short_call['StrikePrice'].iloc[0], " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                        short_call_itm = True

                    #close short call
                    cash_balance -= short_call['AskPrice'].iloc[0] * 100
                    balance_invested += short_call['AskPrice'].iloc[0] * 100
                    print("CLOSING SHORT CALLS: ", short_call['AskPrice'].iloc[0])
                    premium_collected -= short_call['AskPrice'].iloc[0] * 100
                    short_option = None
                    num_short_closes += 1

                    ############### dates switch formats in 2019 ##################################
                    current_dt = datetime.strptime(day, '%Y-%m-%d').date() if ('/' not in day) else datetime.strptime(day, '%m/%d/%Y').date()
                    short_exp_date = None
                    for date in exps:
                        ############### dates switch formats in 2019 ##################################
                        exp_date = datetime.strptime(date, '%Y-%m-%d').date() if ('/' not in date) else datetime.strptime(date, '%m/%d/%Y').date()
                        if (exp_date - current_dt).days >= 6:
                            short_exp_date = date
                            break

                    if short_exp_date is not None:
                        if cost_basis is not None:
                            short_calls = options_chain.loc[(options_chain['ExpirationDate'] == short_exp_date) & (options_chain['PutCall'] == "call") & (abs(options_chain['Delta']) < .5) & (options_chain['StrikePrice'] >= cost_basis) & (options_chain['BidPrice'] >= 0.1)].drop_duplicates()
                        else:
                            short_calls = options_chain.loc[(options_chain['ExpirationDate'] == short_exp_date) & (options_chain['PutCall'] == "call") & (abs(options_chain['Delta']) < .5) & (options_chain['BidPrice'] >= 0.1)].drop_duplicates()
                        call_index = 0
                        delta_diff = 1000
                        for call_ind in short_calls.index:
                            if isinstance(short_calls['Delta'][call_ind], (int, float)):
                                abs_diff = abs(abs(short_calls['Delta'][call_ind]) - .16)
                                if abs_diff < delta_diff:
                                    call_index = call_ind
                                    delta_diff = abs_diff
                        if short_calls.empty:
                            result_data.append({'Date': day, 'AccountValue': (cash_balance + balance_invested)})
                            print(day, " cash: $", cash_balance, " invested: $", balance_invested, " total value: $", (cash_balance + balance_invested), " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                            continue
                        #sell order
                        if short_calls['BidPrice'][call_index] > 0:
                            print("short call exp: ", short_calls['ExpirationDate'][call_index], ", Delta: ", short_calls['Delta'][call_index], " strike: ", short_calls['StrikePrice'][call_index], " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                            premium_collected += short_calls['BidPrice'][call_index] * 100
                            short_option = Option('SPY', short_calls['ExpirationDate'][call_index], short_calls['StrikePrice'][call_index], True)
                            last_short_price = short_calls['BidPrice'][call_index]
                            print("short call premium: ", short_calls['BidPrice'][call_index])
                            cash_balance += short_calls['BidPrice'][call_index] * 100
                            balance_invested -= short_call['BidPrice'].iloc[0] * 100
                            num_short_sells += 1


                #if there's a new expirtion or the short call is expiring itm roll the long call
                if exps[-1] > long_option.exp_date or short_call_itm:
                    print("four")
                    #close long position
                    cash_balance += long_call['BidPrice'].iloc[0] * 100
                    balance_invested -= long_call['BidPrice'].iloc[0] * 100
                    #cost_basis = None
                    num_long_closes += 1
                    print("rolling long call")

                    long_calls = options_chain.loc[(options_chain['ExpirationDate'] == exps[-1]) & (options_chain['PutCall'] == "call") & (abs(options_chain['Delta']) > .5)].drop_duplicates()
                    call_index = 0
                    delta_diff = 1000
                    for call_ind in long_calls.index:
                        if isinstance(long_calls['Delta'][call_ind], (int, float)):
                            abs_diff = abs(abs(long_calls['Delta'][call_ind]) - .9)
                            if abs_diff < delta_diff:
                                call_index = call_ind
                                delta_diff = abs_diff
                    if long_calls.empty:
                        result_data.append({'Date': day, 'AccountValue': (cash_balance + balance_invested)})
                        print(day, " cash: $", cash_balance, " invested: $", balance_invested, " total value: $", (cash_balance + balance_invested), " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                        continue
                    if long_calls['BidPrice'][call_ind] < (long_calls['UnderlyingPrice'][call_ind] - long_calls['StrikePrice'][call_ind]):
                        print("skipped bad mark")
                        result_data.append({'Date': day, 'AccountValue': (cash_balance + balance_invested)})
                        print(day, " cash: $", cash_balance, " invested: $", balance_invested, " total value: $", (cash_balance + balance_invested), " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                        continue

                    #buy order
                    print("Long call exp: ", exps[-1], ", Delta: ", long_calls['Delta'][call_index], " strike: ", long_calls['StrikePrice'][call_index], " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                    long_option = Option('SPY', long_calls['ExpirationDate'][call_index], long_calls['StrikePrice'][call_index], True)
                    last_long_price = long_calls['AskPrice'][call_index]
                    cash_balance -= long_call['AskPrice'].iloc[0] * 100
                    balance_invested += long_call['AskPrice'].iloc[0] * 100
                    #cost_basis = long_calls['UnderlyingPrice'][call_index]
                    num_long_buys += 1
        elif long_option is not None:
            exps = options_chain['ExpirationDate'].unique()
            long_call = options_chain.loc[(options_chain['ExpirationDate'] == long_option.exp_date) & (options_chain['PutCall'] == "call") & (abs(options_chain['StrikePrice']) == long_option.strike)]

            #if we can't find our strike anymore close everything and reopen #####################################################
            current_position_not_found = False
            if long_call.empty:
                 current_position_not_found = True
            #if there is a bad mark for a long call just close it to avoid issues with the data
            bad_mark = False
            if not long_call.empty:
                if long_call['BidPrice'].iloc[0] < (long_call['UnderlyingPrice'].iloc[0] - long_call['StrikePrice'].iloc[0]):
                    print("closing due to bad mark")
                    bad_mark = True

            if current_position_not_found or bad_mark:
                long_option = None
                if not long_call.empty:
                    if bad_mark:
                        cash_balance += last_long_price * 100
                        balance_invested -= last_long_price * 100
                    else:
                        cash_balance += long_call['BidPrice'].iloc[0] * 100
                        balance_invested -= long_call['BidPrice'].iloc[0] * 100
                else:
                    cash_balance += last_long_price * 100
                    balance_invested -= last_long_price * 100
                last_long_price = None
                #cost_basis = None
                num_long_closes += 1
                print("Couldn't find the current position anymore. Closing all positions")
            else:
                #update portfolio value
                balance_invested = (long_call['BidPrice'].iloc[0] * 100)

                #if there's a new expirtion or the short call is expiring itm roll the long call
                if exps[-1] > long_option.exp_date:
                    print("four")
                    #close long position
                    cash_balance += long_call['BidPrice'].iloc[0] * 100
                    balance_invested -= long_call['BidPrice'].iloc[0] * 100
                    #cost_basis = None
                    num_long_closes += 1
                    print("rolling long call")

                    long_calls = options_chain.loc[(options_chain['ExpirationDate'] == exps[-1]) & (options_chain['PutCall'] == "call") & (abs(options_chain['Delta']) > .5)].drop_duplicates()
                    call_index = 0
                    delta_diff = 1000
                    for call_ind in long_calls.index:
                        if isinstance(long_calls['Delta'][call_ind], (int, float)):
                            abs_diff = abs(abs(long_calls['Delta'][call_ind]) - .9)
                            if abs_diff < delta_diff:
                                call_index = call_ind
                                delta_diff = abs_diff
                    if long_calls.empty:
                        result_data.append({'Date': day, 'AccountValue': (cash_balance + balance_invested)})
                        print(day, " cash: $", cash_balance, " invested: $", balance_invested, " total value: $", (cash_balance + balance_invested), " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                        continue
                    if long_calls['BidPrice'][call_ind] < (long_calls['UnderlyingPrice'][call_ind] - long_calls['StrikePrice'][call_ind]):
                        print("skipped bad mark")
                        result_data.append({'Date': day, 'AccountValue': (cash_balance + balance_invested)})
                        print(day, " cash: $", cash_balance, " invested: $", balance_invested, " total value: $", (cash_balance + balance_invested), " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                        continue

                    #buy order
                    print("Long call exp: ", exps[-1], ", Delta: ", long_calls['Delta'][call_index], " strike: ", long_calls['StrikePrice'][call_index], " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                    long_option = Option('SPY', long_calls['ExpirationDate'][call_index], long_calls['StrikePrice'][call_index], True)
                    last_long_price = long_calls['AskPrice'][call_index]
                    cash_balance -= long_call['AskPrice'].iloc[0] * 100
                    balance_invested += long_call['AskPrice'].iloc[0] * 100
                    #cost_basis = long_calls['UnderlyingPrice'][call_index]
                    num_long_buys += 1

        if long_option is None:
            print("five")
            exps = options_chain['ExpirationDate'].unique()
            long_calls = options_chain.loc[(options_chain['ExpirationDate'] == exps[-1]) & (options_chain['PutCall'] == "call") & (abs(options_chain['Delta']) > .5)].drop_duplicates()
            call_index = 0
            delta_diff = 1000
            for call_ind in long_calls.index:
                if isinstance(long_calls['Delta'][call_ind], (int, float)):
                    abs_diff = abs(abs(long_calls['Delta'][call_ind]) - .9)
                    if abs_diff < delta_diff:
                        call_index = call_ind
                        delta_diff = abs_diff
            if long_calls.empty:
                result_data.append({'Date': day, 'AccountValue': (cash_balance + balance_invested)})
                print(day, " cash: $", cash_balance, " invested: $", balance_invested, " total value: $", (cash_balance + balance_invested), " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                continue
            if long_calls['BidPrice'][call_ind] < (long_calls['UnderlyingPrice'][call_ind] - long_calls['StrikePrice'][call_ind]):
                print("skipped bad mark 2")
                result_data.append({'Date': day, 'AccountValue': (cash_balance + balance_invested)})
                print(day, " cash: $", cash_balance, " invested: $", balance_invested, " total value: $", (cash_balance + balance_invested), " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                continue

            #buy order
            print("Long call exp: ", exps[-1], ", Delta: ", long_calls['Delta'][call_index], " strike: ", long_calls['StrikePrice'][call_index], " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
            long_option = Option('SPY', long_calls['ExpirationDate'][call_index], long_calls['StrikePrice'][call_index], True)
            last_long_price = long_calls['AskPrice'][call_index]
            cash_balance -= long_calls['AskPrice'][call_index] * 100
            balance_invested += long_calls['AskPrice'][call_index] * 100
            cost_basis = long_calls['UnderlyingPrice'][call_index]
            num_long_buys += 1

        if short_option is None:
            print("six")
            exps = options_chain['ExpirationDate'].unique()

            ############### dates switch formats in 2019 ##################################
            current_dt = datetime.strptime(day, '%Y-%m-%d').date() if ('/' not in day) else datetime.strptime(day, '%m/%d/%Y').date()
            short_exp_date = None
            for date in exps:
                ############### dates switch formats in 2019 ##################################
                exp_date = datetime.strptime(date, '%Y-%m-%d').date() if ('/' not in date) else datetime.strptime(date, '%m/%d/%Y').date()
                if (exp_date - current_dt).days >= 6:
                    short_exp_date = date
                    break

            if short_exp_date is not None:
                if cost_basis is not None:
                    short_calls = options_chain.loc[(options_chain['ExpirationDate'] == short_exp_date) & (options_chain['PutCall'] == "call") & (abs(options_chain['Delta']) < .5) & (options_chain['StrikePrice'] >= cost_basis) & (options_chain['BidPrice'] >= 0.1)].drop_duplicates()
                else:
                    short_calls = options_chain.loc[(options_chain['ExpirationDate'] == short_exp_date) & (options_chain['PutCall'] == "call") & (abs(options_chain['Delta']) < .5) & (options_chain['BidPrice'] >= 0.1)].drop_duplicates()

                call_index = 0
                delta_diff = 1000
                for call_ind in short_calls.index:
                    if isinstance(short_calls['Delta'][call_ind], (int, float)):
                        abs_diff = abs(abs(short_calls['Delta'][call_ind]) - .16)
                        if abs_diff < delta_diff:
                            call_index = call_ind
                            delta_diff = abs_diff
                if short_calls.empty:
                    print("empty")
                    result_data.append({'Date': day, 'AccountValue': (cash_balance + balance_invested)})
                    print(day, " cash: $", cash_balance, " invested: $", balance_invested, " total value: $", (cash_balance + balance_invested), " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                    continue
                #sell order
                if short_calls['BidPrice'][call_index] > 0:
                    print("Short call exp: ", short_calls['ExpirationDate'][call_index], ", Delta: ", short_calls['Delta'][call_index], " strike: ", short_calls['StrikePrice'][call_index], " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")
                    premium_collected += short_calls['BidPrice'][call_index] * 100
                    short_option = Option('SPY', short_calls['ExpirationDate'][call_index], short_calls['StrikePrice'][call_index], True)
                    last_short_price = short_calls['BidPrice'][call_index]
                    print("short call premium: ", short_calls['BidPrice'][call_index])
                    cash_balance += short_calls['BidPrice'][call_index] * 100
                    balance_invested -= short_calls['BidPrice'][call_index] * 100
                    num_short_sells += 1

        account_value_per_day.append(cash_balance + balance_invested)
        result_data.append({'Date': day, 'AccountValue': (cash_balance + balance_invested)})
        print(day, " cash: $", cash_balance, " invested: $", balance_invested, " total value: $", (cash_balance + balance_invested), " stock price: ", spy_prices_df['Close'][day] if ('/' not in day and day in spy_prices_df.index) else "none")

    print("seven")
    keys = ['Date', 'AccountValue']

    with open('spy_backtest_results2.csv', 'a') as output_file:
        dict_writer = csv.DictWriter(output_file, restval="-", fieldnames=keys, delimiter=',')
        dict_writer.writeheader()
        dict_writer.writerows(result_data)

    end = timer()
    print(end - start) # Time in seconds, e.g. 5.38091952400282

    print("number of short sells: ", num_short_sells)
    print("number of shorts bought back: ", num_short_closes)
    print("number of long buys: ", num_long_buys)
    print("number of long sold again ", num_long_closes)
    print("total premium collected: ", premium_collected)

#plots results from backtest 2
def plot_results2():
    df = pd.read_csv('spy_backtest_results2.csv').drop_duplicates()
    #df['Date'] = df['Date'].apply(lambda d: d.date().strftime('%Y/%m/%d'))
    df = df.set_index('Date')
    df.index = df.index.astype(str)
    spy_prices = yf.download('SPY', '2005-01-03', '2021-12-31')
    spy_prices = spy_prices[~spy_prices.index.duplicated(keep='last')]
    temp = []

    for i in spy_prices.index:
        temp.append(i.date().strftime('%Y-%m-%d'))

    spy_prices['DateStr'] = temp
    spy_prices = spy_prices.set_index('DateStr')
    spy_prices.index = spy_prices.index.astype("string")

    spy_prices_match = []
    dates = []
    datetimes = []

    df = df.drop_duplicates()
    spy_prices = spy_prices.drop_duplicates()

    for date in df.index.drop_duplicates():
        #print(date)
        #print(df.index.dtype)
        if date in spy_prices.index:
            print(spy_prices['Close'][date])
            spy_prices_match.append(spy_prices['Close'][date])
            dates.append(date)
            datetimes.append(datetime.strptime(date, '%Y-%m-%d'))

    print(df['AccountValue']['2021-12-30'])
    print(spy_prices['Close']['2021-12-30'])

    #normalize spy prices for the size of the portfolio
    factor = 100000 / spy_prices['Close'][dates[0]]
    print(factor)
    for i in range(len(spy_prices_match)):
        spy_prices_match[i] = spy_prices_match[i] * factor

    account_vals = df['AccountValue'][df.index.isin(dates)].tolist()
    print(len(account_vals), " ", len(dates))
    temp_df = df['AccountValue'][df.index.isin(dates)].drop_duplicates()
    prices = spy_prices['Close'][spy_prices.index.isin(temp_df.index)]

    print(len(account_vals), " ", len(spy_prices_match), " ", len(datetimes), " ", len(dates))
    for i in range(len(account_vals)):
        #print(account_vals[i])
        account_vals[i] = 100000 + ((float(account_vals[i]) - 100000) * 5)


    #plt.plot(datetimes, spy_prices_match, label="SPY")
    #plt.plot(datetimes, account_vals, label="Backtest")
    df = df[~df.index.duplicated(keep='first')]
    plt.plot(datetimes, df['AccountValue'][df.index.isin(dates)], label="Backtest")
    plt.show()

def plot_results2():
    df = pd.read_csv('spy_backtest_results2.csv').drop_duplicates()
    #df['Date'] = df['Date'].apply(lambda d: d.date().strftime('%Y/%m/%d'))
    df = df.set_index('Date')
    df.index = df.index.astype(str)
    spy_prices = yf.download('SPY', '2005-01-03', '2021-12-31')
    spy_prices.index = spy_prices.index.astype(str)

    print(df)
    print(spy_prices)

    df3 = df.merge(spy_prices, how="inner", left_index=True, right_index=True)
    print(df3)

    dates_list = [datetime.strptime(date, '%Y-%m-%d').date() for date in df3.index]
    print(dates_list)

    factor = 100000 / spy_prices['Close'][df.index.tolist()[0]]
    df3['Close'] = df3['Close'].apply(lambda x: x*factor)
    df3['AccountValue'] = df3['AccountValue'].apply(lambda x: 100000 + ((float(x) - 100000) * 5))

    plt.plot(dates_list, df3['AccountValue'].tolist(), label="Backtest")
    plt.plot(dates_list, df3['Close'].tolist(), label="SPY")
    plt.show()

#backtest_pmcc1_spy2()
#plot_results2()









######################### vix term structure historical data ##############################
## TODO: compare vix to vix 3 month and see if there are any days where 3 month was lower than vix (backwardation)

vix_prices = pdrd.get_data_yahoo('^VIX', '2005-01-03', '2021-12-31')
vix3m_prices = pdrd.get_data_yahoo('^VIX3M', '2005-01-03', '2021-12-31')
#vix_prices = yf.download('^VIX3M', '2005-01-03', '2021-12-31')

temp = []
for i in vix_prices.index:
    temp.append(i.date().strftime('%Y-%m-%d'))
print('2008-06-26' in temp)
vix_prices['DateStr'] = temp
vix_prices = vix_prices.set_index('DateStr')
vix_prices.index = vix_prices.index.astype(str)

print(temp)

print(vix_prices)
print(vix3m_prices)

print('2008-06-26' in vix_prices.index)
print(len(vix_prices.index.tolist()), " ", len(vix3m_prices.index.tolist()))

print(vix_prices.index.dtype)
print("vix")
print(vix_prices.loc['2008-06-27']['Close'])
print("vix3")
print(vix3m_prices.loc['2008-06-27']['Close'])

######## to do ##################3
# change save data pkl function to convert the dates in 2019 and after to string format
# change expiration dates at end of the week to be fridays instead of saturdays
