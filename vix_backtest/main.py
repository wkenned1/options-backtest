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
import statistics


def saveOptionData():
    #based on folder structure of the option data
    #second level XXXX_XG
    #third level XXXXMarch
    single_dataset_years = ['2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023']

    days_list = []
    dataframes = []

    base_path = "../../../historical_vix_term_structure/"

    for i in single_dataset_years:
        path = base_path
        path += i
        path += "/"
        file_names = os.listdir(path)
        print(file_names)
        for k in file_names:
            path2 = path + k
            '''raw_date = k.split("_")[1]
            date = raw_date[0:4] + "-" + raw_date[4:6] + "-" + raw_date[6:8]
            days_list.append(date)'''
            #print(path2)
            dataframes.append(readCsv(path2))
            #print("file read")

    total_data = pd.concat(dataframes)
    print(total_data)
    return total_data
    #data_obj = HistoricalData(ticker, days_list[:], total_data)
    #return data_obj


def readCsv(path):
    #data = pd.read_csv(path, skiprows= lambda x: isinstance(data.iloc[[x]]['ImpliedVolatility'], str), dtype={'OpenInterest': int, 'UnderlyingPrice': float, 'Volume': int, 'ImpliedVolatility': float, 'StrikePrice': float})
    data = pd.read_csv(path, on_bad_lines='skip')
    #ret = data[data['ImpliedVolatility'].apply(lambda x: not isinstance(x, str))]
    return data

def stockDataDF(ticker, startDate, endDate):
    spy_prices = yf.download(ticker, startDate, endDate)
    spy_prices_df = pd.DataFrame(spy_prices, columns=['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])
    dates = spy_prices_df.index
    string_dates = []
    for i in dates:
        string_dates.append(i.strftime('%Y-%m-%d'))
    spy_prices_df.index = string_dates
    return spy_prices_df

def sharpe_ratio(return_series, N, rf):
    total_profit = (return_series[-1] - return_series[0]) / return_series[0]
    mean = (sum(return_series) / len(return_series)) * N -rf
    sigma = statistics.stdev(return_series) * np.sqrt(N)
    #return mean / sigma
    return (total_profit - rf) / sigma

## read vic csv files
data = saveOptionData()

## convert all dates to same format
dates = data['Trade Date']
strings = []
for t in dates:
    try:
        if '/' in t:
            split_str = t.split('/')
            strings.append(split_str[2] + "-" + split_str[0] + "-" + split_str[1])
        else:
            strings.append(t)
        #strings.append(t.replace('/', '-'))
    except:
        print("error ", t)
        break

data['Trade Date'] = strings
data = data.set_index('Trade Date')

## get spy prices
spy_prices = stockDataDF('SPY', '2005-01-03', '2022-10-31')

## remove duplicates, fix incorrect formatting for dates
unique_dates = list(set(data.index.tolist()))
unique_dates = [d for d in unique_dates if d >= '2006-01-03' and d <= '2022-10-31']
for i in range(len(unique_dates)):
    if len(unique_dates[i]) < 10:
        temp = unique_dates[i].split('-')
        if len(temp[1]) < 2:
            temp[1] = "0" + temp[1]
        if len(temp[2]) < 2:
            temp[2] = "0" + temp[2]
        unique_dates[i] = temp[0] + "-" + temp[1] + "-" + temp[2]
unique_dates = [i for i in unique_dates if i in spy_prices.index]
unique_dates = list(set(unique_dates))
unique_dates.sort()
print(unique_dates)

## keep only days that match the historical vix data
filtered_spy_prices = spy_prices.loc[spy_prices.index.isin(unique_dates)]
print(filtered_spy_prices)
print(len(unique_dates), " ", len(filtered_spy_prices.index))

print([i for i in unique_dates if len(i) < 10])

spy_prices_new = []
error_count = 0
error_count2 = 0
in_contango = []
for s in unique_dates:
    temp_df = data.loc[s]
    #print(s)
    #print(temp_df)
    if data.index.tolist().count(s) <= 1:
        print("not enough data")
        error_count2 += 1
        spy_prices_new.append(50)
        in_contango.append(True)
    else:
        #print(len(temp_df.index))
        #backwardation
        if temp_df['Open'].iloc[0] > temp_df['Open'].iloc[1]:
            spy_prices_new.append(0)
            in_contango.append(False)
        #contango
        else:
            try:
                spy_prices_new.append(spy_prices['Close'].loc[s])
                in_contango.append(True)
            except:
                print("key error")
                error_count += 1
                spy_prices_new.append(True)

print(error_count)
print(error_count2)

porfolio_value = 100000
num_shares = 0
cost_basis = 0

portfolio_values = []
spy_values = []
portfolio_values.append(100000)
spy_values.append(100000)
num_shares = 100000 / filtered_spy_prices['Open'].iloc[0]

factor = 100000 / filtered_spy_prices['Open'].iloc[0]

#begin backtest
for i in range(1, len(unique_dates)):
    spy_price = filtered_spy_prices['Open'].iloc[i]
    spy_values.append(factor * spy_price)
    if num_shares > 0:
        portfolio_value = spy_price * num_shares
        portfolio_values.append(portfolio_value)
    else:
        portfolio_values.append(portfolio_value)
    if in_contango[i]:
        if num_shares == 0:
            #buy shares
            num_shares = portfolio_value / filtered_spy_prices['Open'].iloc[i]
    else:
        if num_shares > 0:
            #sell shares
            num_shares = 0

print("Biggest drawdown: ", ((min(portfolio_values) - 100000) / 100000) * 100, "%")
print("Sharpe ratio: ", sharpe_ratio(portfolio_values, 255, 0.01))
print("Biggest drawdown: ", ((min(spy_values) - 100000) / 100000) * 100, "%")
print("Sharpe ratio: ", sharpe_ratio(spy_values, 255, 0.01))
print("Profit: $", portfolio_values[-1] - portfolio_values[0])
print("SPY Profit: $", spy_values[-1] - spy_values[0])

datetimes = [datetime.strptime(date, '%Y-%m-%d').date() for date in unique_dates]
plt.plot(datetimes, portfolio_values, label="Backtest")
plt.plot(datetimes, spy_values, label="SPY")
plt.show()
