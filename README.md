# Options trading strategy backtester

## Description

This project includes backtests for a variety of trading strategies written in Python. The main strategies tested are variants of a Synthetic Covered Call strategy, also called Poor Man's Covered Call or PMCC. The strategy involves taking a synthetic long position in an underlying by buying a long dated call option, usually with high delta. Shorter dated call options are then sold against the long call option to generate income. The trade typically has net positive theta, delta, and vega.

### Other Strategies included

There are also functions written for testing long SPY trades with entry/exits based on the VIX term structure, and a SPY put hedging strategy.

### Setup

The options data used in the backtests was provided by [Discount Option Data](https://www.discountoptiondata.com/). To save time, I parsed the data for SPY options into a dataframe and pickled it. These functions are also included in ```main.py```. 

To save more time, I wrote ```import_data.py``` to keep a thread running with the dataframe after reading the pickle file, as this step alone could take over a minute each time. 

### How the backtest works

The backtests track daily portfolio balance starting from an arbitrary portfolio value. The daily P&L and other metrics can be derived from the time series data. The synthetic covered call backtests include settings for varying the percentage of the portfolio invested, as well as delta and expiration requirements for entering trades.

The backtesting script keeps track of every individual position as it steps through each trading day. It tracks total premium collected and total slippage also.

## Results

Admittedly, the results are a bit disorganized, as I didn't intend to make this project public originally. However, I had some of the backtests categorized in this [Google Drive](https://drive.google.com/drive/folders/10_52i1jXTYihCW8Yb6YqzTJS8zBmNyJ_?usp=sharing). 
