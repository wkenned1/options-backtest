import numpy as np
#import tensorflow
import pandas as pd
import pandas_datareader as pdr
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

start = timer()
spy_prices = yf.download('SPY', '2005-01-03', '2021-12-31')
vix_prices = yf.download('^VIX', '2005-01-03', '2021-12-31')
data_obj = load_historical_data('SPY')
end = timer()
print(end - start) # Time in seconds, e.g. 5.38091952400282

trading_days = data_obj.data['DataDate'].unique()
print(trading_days)

start = timer()

long_option = None
short_option = None
last_long_price = None
last_short_price = None

account_value = 100000

num_long_buys = 0
num_long_closes = 0
num_short_sells = 0
num_short_closes = 0

account_value_per_day = []

for day in trading_days:
    ############## skipping some missing data for spy #####################
    #if '2005-01-21' > day:
    if '2005-01-21' > day and '/' not in day:
        #print("skipping: ", day)
        account_value_per_day.append(account_value)
        continue
    elif '2021-11-17' == day:
        print("done skipping")

    options_chain = data_obj.data.loc[data_obj.data['DataDate'] == day]

    ##### if we already have a position
    if short_option is not None and long_option is not None:
        #track portfolio value
        long_call = options_chain.loc[(options_chain['ExpirationDate'] == long_option.exp_date) & (options_chain['PutCall'] == "call") & (abs(options_chain['StrikePrice']) == long_option.strike)]
        short_call = options_chain.loc[(options_chain['ExpirationDate'] == short_option.exp_date) & (options_chain['PutCall'] == "call") & (abs(options_chain['StrikePrice']) == short_option.strike)]

        #if we can't find our strike anymore close everything and reopen #####################################################
        current_position_not_found = False
        if long_call.empty or short_call.empty:
             current_position_not_found = True

        if current_position_not_found:
            long_option = None
            last_long_price = None
            short_option = None
            last_short_price = None
            num_long_closes += 1
            num_short_closes += 1
            print("Couldn't find the current position anymore. Closing all positions")
        else:
            account_value += ((long_call['BidPrice'].iloc[0] - last_long_price) * 100)

            ###################### remove later ############################3
            #print("Testing: ")
            #print(short_call)
            ################################################################333

            account_value -= ((short_call['AskPrice'].iloc[0] - last_short_price) * 100)
            last_long_price = long_call['BidPrice'].iloc[0]
            last_short_price = short_call['AskPrice'].iloc[0]

            exps = options_chain['ExpirationDate'].unique()

            short_call_itm = False
            current_dt = datetime.strptime(day, '%Y-%m-%d').date() if ('/' not in day) else datetime.strptime(day, '%m/%d/%Y').date()
            current_exp_date = datetime.strptime(short_option.exp_date, '%Y-%m-%d').date() if ('/' not in short_option.exp_date) else datetime.strptime(short_option.exp_date, '%m/%d/%Y').date()
            #if the short call expires today roll it
            if day == short_option.exp_date or (current_dt.weekday() == 4 and (current_exp_date - current_dt).days == 1):
                #if in the money, close entire position
                if abs(short_call['Delta'].iloc[0]) > .5:
                    num_long_closes += 1
                    short_call_itm = True

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
                    short_calls = options_chain.loc[(options_chain['ExpirationDate'] == short_exp_date) & (options_chain['PutCall'] == "call") & (abs(options_chain['Delta']) < .5)]
                    call_index = 0
                    delta_diff = 1000
                    for call_ind in short_calls.index:
                        abs_diff = abs(abs(short_calls['Delta'][call_ind]) - .16)
                        if abs_diff < delta_diff:
                            call_index = call_ind
                            delta_diff = abs_diff
                    print("short call exp: ", short_calls['ExpirationDate'][call_index], ", Delta: ", short_calls['Delta'][call_index])
                    short_option = Option('SPY', short_calls['ExpirationDate'][call_index], short_calls['StrikePrice'][call_index], True)
                    last_short_price = short_calls['BidPrice'][call_index]
                    num_short_sells += 1


            #if there's a new expirtion or the short call is expiring itm roll the long call
            if exps[-1] > long_option.exp_date or short_call_itm:
                num_long_closes += 1

                long_calls = options_chain.loc[(options_chain['ExpirationDate'] == exps[-1]) & (options_chain['PutCall'] == "call") & (abs(options_chain['Delta']) > .5)]
                call_index = 0
                delta_diff = 1000
                for call_ind in long_calls.index:
                    abs_diff = abs(abs(long_calls['Delta'][call_ind]) - .9)
                    if abs_diff < delta_diff:
                        call_index = call_ind
                        delta_diff = abs_diff
                print("Long call exp: ", exps[-1], ", Delta: ", long_calls['Delta'][call_index])
                long_option = Option('SPY', long_calls['ExpirationDate'][call_index], long_calls['StrikePrice'][call_index], True)
                last_long_price = long_calls['AskPrice'][call_index]
                num_long_buys += 1

    if long_option is None:
        exps = options_chain['ExpirationDate'].unique()
        long_calls = options_chain.loc[(options_chain['ExpirationDate'] == exps[-1]) & (options_chain['PutCall'] == "call") & (abs(options_chain['Delta']) > .5)]
        call_index = 0
        delta_diff = 1000
        for call_ind in long_calls.index:
            abs_diff = abs(abs(long_calls['Delta'][call_ind]) - .9)
            if abs_diff < delta_diff:
                call_index = call_ind
                delta_diff = abs_diff
        print("Long call exp: ", exps[-1], ", Delta: ", long_calls['Delta'][call_index])
        long_option = Option('SPY', long_calls['ExpirationDate'][call_index], long_calls['StrikePrice'][call_index], True)
        last_long_price = long_calls['AskPrice'][call_index]
        num_long_buys += 1

    if short_option is None:
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
            short_calls = options_chain.loc[(options_chain['ExpirationDate'] == short_exp_date) & (options_chain['PutCall'] == "call") & (abs(options_chain['Delta']) < .5)]
            call_index = 0
            delta_diff = 1000
            for call_ind in short_calls.index:
                abs_diff = abs(abs(short_calls['Delta'][call_ind]) - .16)
                if abs_diff < delta_diff:
                    call_index = call_ind
                    delta_diff = abs_diff
            print("Short call exp: ", short_calls['ExpirationDate'][call_index], ", Delta: ", short_calls['Delta'][call_index])
            short_option = Option('SPY', short_calls['ExpirationDate'][call_index], short_calls['StrikePrice'][call_index], True)
            last_short_price = short_calls['BidPrice'][call_index]
            num_short_sells += 1

    account_value_per_day.append(account_value)
    print(day, " $", account_value)


keys = ['Date', 'AccountValue']
result_data = []
for i in range(len(trading_days)):
    result_data.append({'Date': trading_days[i], 'AccountValue': account_value_per_day[i]})

with open('spy_backtest_results.csv', 'a') as output_file:
    dict_writer = csv.DictWriter(output_file, restval="-", fieldnames=keys, delimiter=',')
    dict_writer.writeheader()
    dict_writer.writerows(result_data)

'''for i in data_obj.data.index:
    if data_obj.data['DataDate'][i] > current_date:
        current_date = data_obj.data['DataDate'][i]

    if long_option is None:
        continue

    if short_option is None:
        continue

    if long_option is not None and short_option is not None:

        #check if the current option is being held in the portfolio to update portfolio value
        #if the short call's expiration date equals current_date, roll the short option

'''

end = timer()
print(end - start) # Time in seconds, e.g. 5.38091952400282

print("number of short sells: ", num_short_sells)
print("number of shorts bought back: ", num_short_closes)
print("number of long buys: ", num_long_buys)
print("number of long sold again ", num_long_closes)
