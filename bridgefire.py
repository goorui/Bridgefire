import quandl
import datetime
import math
import pandas as pd
import numpy as np

from sklearn.svm import SVR
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import learning_curve
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor

# helper functions
#
#
#

# function to replace abnormal values and assign industry according to range
def sic_hash(sic):
    if sic < 100:
        return -1
    for i in range(len(sic_range_mins)-1):  # i goes from 0 to len-2
        if sic < sic_range_mins[i+1]:
            return sic_range_mins[i]
    return -2

# function to test whether two stocks are in the same industry
def test(s, model):
    if s == model:
        return 1
    return 0

# login
quandl.save_key('J3AKfEcoxgiV2TtMr2ue')
quandl.read_key()

# repeatedly get all data and convert to pandas data frame
df = pd.DataFrame([])

cursor_id = None
while True:
    data = quandl.Datatable('SHARADAR/SF1').data(params={'qopts':
    {'columns': [
        'ticker', 'dimension', 'calendardate', 'netinc', 'marketcap', 'assets', 'liabilities', 'netinc', 'rnd', 'revenue'],
    'cursor_id': cursor_id}})
    cursor_id = data.meta['next_cursor_id']
    df = df.append(data.to_pandas())
    if cursor_id is None:
        break

# we only need annual data
df = df[df['dimension'] == 'MRY']
# print(df.shape)

# sort the frame according to ticker and calendar date and data cleaning
df = df.sort_values(by=['ticker', 'calendardate'])

# if there's duplicate statements, we only keep the latest one
df = df.drop_duplicates(keep='last')
# after dropping duplicates, there're about 8748 rows

# because we want to use logarithm afterwards, we replace all non-positive value with 0
df['rnd'] = df['rnd'].where(df['rnd'] > 0, 0)

# create new columns in the frame following the tutorial, we replace log_NC with NC
df = df.assign(NC=np.log(df['assets'] - df['liabilities'])) #公司对数净资产
df = df.assign(LEV=df['assets'] / df['liabilities']) # QUESTIONS1 ! 公司财务杠杆
df = df.assign(log_mcap=np.log(df['marketcap'])) # 公司对数净市值

df = df.assign(NI_p = df['netinc'].where(df['netinc'] > 0))
df = df.assign(NI_n = df['netinc'].where(df['netinc'] < 0))

df = df.assign(log_RD=np.log(df['rnd']))

# Question 2!
# calculate increase in revenue
df = df.assign(g=np.nan)
df_group_by_ticker = df.groupby('ticker')
# create a new data frame to accommodate the new column g
df = pd.DataFrame([])
for group in df_group_by_ticker:
    length = len(group[1].index)
    for i in range(1, length):
        group[1].at[group[1].index[i], 'g'] = group[1].at[group[1].index[i],
                                                          'revenue'] - group[1].at[group[1].index[i-1], 'revenue']
    df = df.append(group[1])

# Question3 !
# only keep desired columns
#df = df.drop(columns = ['dimension', 'marketcap', 'netinc', 'assets', 'liabilities', 'rnd', 'revenue'])

data = quandl.Datatable(
    'SHARADAR/TICKERS').data(params={'qopts': {'columns': ['ticker', 'siccode']}})
df_tickers = data.to_pandas()

sic_range_mins = [100, 1000, 1500, 1800, 2000,
                  4000, 5000, 5200, 6000, 7000, 9100, 9900, 10000]

sic_dict = {-1: "Error_low",
            -2: "Error_high",
            100: "Agriculture, Forestry and Fishing",
            1000: "Mining",
            1500: "construction",
            1800: "no used",
            2000: "Manufacturing",
            4000: "Transportation, Communications, Electric, Gas and Sanitary service",
            5000: "Wholesale Trade",
            5200: "Retail Trade",
            6000: "Finance, Insurance and Real Estate",
            7000: "Services",
            9100: "Publi Adminstration",
            9900: "Nonclassifiable"}

# creating a single column, in each row apply sic_hash to the siccode
df_tickers['sic_hash'] = df_tickers.apply(lambda row: sic_hash(row['siccode']), axis=1)
df_tickers['industry'] = df_tickers.apply(lambda row: sic_dict[row['sic_hash']], axis=1)

# change the column to an array of unique values
unique_sic_hashes = df_tickers['sic_hash'].unique()

for sh in unique_sic_hashes:
    colname = "industry: " + sic_dict[sh]
    df_tickers[colname] = df_tickers.apply(lambda row: test(row['sic_hash'], sh), axis=1)

# print(df.head())
print(df.head())
df = df.merge(df_tickers, how='inner', left_on='ticker', right_on='ticker')

# print(df.head())
#print(df_tickers.loc[df_tickers['ticker'] == 'A'])
print(df.head())

#model construction
#six factors and industry sets
X = df[['log_NC', 'LEV', 'NI_p', 'NI_n', 'g', 'log_RD', "100", "1000", "1500", "1800", "2000","4000", "5000", "5200", "6000", "7000", "9100", "9900", "10000"]]
Y = df[['log_mcap']]
X = X.fillna(0)
Y = Y.fillna(0)

#actual fitting
svr = SVR(kernel='rbf', gamma=0.1)
model = svr.fit(X, Y)
factor = Y - pd.DataFrame(svr.predict(X),
    index = Y.index,
    columns = ['log_mcap'])
factor = factor.sort_index(by = 'log_mcap')
stockset = list(factor.index[:10])

print(stockset)
