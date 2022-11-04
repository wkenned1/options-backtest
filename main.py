import numpy as np 
import tensorflow 
import pandas as pd
import pandas_datareader as pdr
import yfinance as yf  
import matplotlib.pyplot as plt
import pickle5 as pickle
import os
 
class HistoricalData:
    #symbol is ticker symbol
    #days_covered is a list of dates in order
    #data is the dataframe
    def __init__(self, symbol, days_covered, data):
        self.symbol = symbol
        self.days_covered = days_covered
        self.data = data

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

    base_path = "/Volumes/Seagate Portable Drive/historical_option_data/"

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


#avg_daily_range('SPY', '1994-01-02', '2022-10-21')
#avg_weekly_range('SPY', '1994-01-02', '2022-10-21')

#saveTickerData('QQQ', '2005-01-03', '2021-12-31')
#print(readTickerData('SPY'))

obj = saveOptionData('SPY')
print(obj.symbol)
print(obj.days_covered)
print(obj.data)

obj.data.to_pickle("/Volumes/Seagate Portable Drive/historical_option_data/spy_options_data.pkl")
with open('/Volumes/Seagate Portable Drive/historical_option_data/spy_options_days.pkl', 'wb') as f:
    pickle.dump(obj.days_covered, f)

#save_object(obj, "spy_options_data.pkl")

