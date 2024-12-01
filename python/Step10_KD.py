'''
計算KD曲線
參考: https://medium.com/%E5%8F%B0%E8%82%A1etf%E8%B3%87%E6%96%99%E7%A7%91%E5%AD%B8-%E7%A8%8B%E5%BC%8F%E9%A1%9E/%E7%A8%8B%E5%BC%8F%E8%AA%9E%E8%A8%80-%E8%87%AA%E5%BB%BAkd%E5%80%BC-819d6fd707c8
資料來源: https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date}&stockNo={stockId}
'''


import datetime
import pandas as pd
import numpy as np

def getRSV():