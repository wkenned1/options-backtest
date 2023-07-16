import pickle as pickle
import os
import numpy as np
import pandas as pd
import pandas_datareader as pdr
from pandas_datareader import data as pdrd
from multiprocessing import Process
import importlib
import threading
import sys 
import trace 
import time 

class thread_with_trace(threading.Thread): 
  def __init__(self, *args, **keywords): 
    threading.Thread.__init__(self, *args, **keywords) 
    self.killed = False
  def start(self): 
    self.__run_backup = self.run 
    self.run = self.__run       
    threading.Thread.start(self) 

  def __run(self): 
    sys.settrace(self.globaltrace) 
    self.__run_backup() 
    self.run = self.__run_backup 
  def globaltrace(self, frame, event, arg): 
    if event == 'call': 
      return self.localtrace 
    else: 
      return None

  def localtrace(self, frame, event, arg): 
    if self.killed: 
      if event == 'line': 
        raise SystemExit() 
    return self.localtrace 
  def kill(self): 
    self.killed = True

class HistoricalData:
    #symbol is ticker symbol
    #days_covered is a list of dates in order
    #data is the dataframe
    def __init__(self, symbol, days_covered, data):
        self.symbol = symbol
        self.days_covered = days_covered
        self.data = data

def load_days_from_pkl(path):
    file = open(path, 'rb')
    data = pickle.load(file)
    file.close()
    return data

def load_historical_data(ticker):
    data = pd.read_pickle("./stored_data/" + ticker + "_options_data.pkl")
    days = load_days_from_pkl("./stored_data/spy_options_days.pkl")
    data_obj = HistoricalData(ticker, days, data)
    return data_obj

def test():
    print('test')

def data_provider(ticker):
    print("reading data")
    data_obj = load_historical_data(ticker)
    print("reading done")
    imported_libs = {}
    threads = []
    mod = None
    while True:
        try:
            filename = input ("enter backtest file or type kill the most recent process:\n").strip()
            if filename == 'kill':
                if len(threads) > 0:
                    if threads[-1].isAlive():
                        threads[-1].kill()
                    threads = threads.pop()
                    print("thread killed, ", len(threads), " threads remaining")
            else:
                if filename not in imported_libs.keys():
                    # if filename not in imported_libs:
                    mod = importlib.import_module(filename)
                    imported_libs[filename] = mod
                else:
                    importlib.reload(imported_libs[filename])
                # mod.backtest_pmcc1_spy2(data_obj)
                t1 = thread_with_trace(target=mod.backtest_pmcc1_spy2, args=(data_obj,)) 
                # t1 = thread_with_trace(target=test) 
                threads.append(t1)
                t1.start()
                t1.join()
                t1.kill()
        except ValueError:
            print ("Something went wrong")
   
data_provider('SPY')    

