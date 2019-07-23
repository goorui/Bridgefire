import quandl
import datetime
import pandas as pd
import numpy as np

quandl.save_key('J3AKfEcoxgiV2TtMr2ue')
quandl.read_key()

# repeatedly get all data and convert to pandas data frame
df = pd.DataFrame([])
cursor_id = None
for i in range(2):  # change to while True in working environment
    data = quandl.Datatable('SHARADAR/SF1').data(params={'qopts': {'columns': [
        'ticker', 'dimension', 'calendardate', 'netinc', 'marketcap', 'assets', 'liabilities', 'netinc', 'rnd', 'revenue'], 'cursor_id': cursor_id}})
    cursor_id = data.meta['next_cursor_id']
    df = df.append(data.to_pandas())
    if cursor_id is None:
        break

# we only need annual data
df = df[df['dimension'] == 'MRY']
print(df.shape)

# sort the frame according to ticker and calendar date and data cleaning
df = df.sort_values(by=['ticker', 'calendardate'])


# if there's duplicate statements, we only keep the latest one
df = df.drop_duplicates(subset='calendardate', keep='last')
# after dropping duplicates, there're about 8748 rows

# because we want to use logarithm afterwards, we replace all non-positive value with 1
df['rnd'] = df['rnd'].where(df['rnd'] > 0, 1)

# create new columns in the frame following the tutorial, we replace log_NC with NC
df = df.assign(NC=df['assets'] - df['liabilities'])
df = df.assign(LEV=df['assets'] / df['liabilities'])
df = df.assign(log_mcap=np.log(df['marketcap']))
df = df.assign(NI_p=np.log(np.abs(df['netinc'])))

df = df.assign(NI_n=df['netinc'] + 1)
df['NI_n'] = np.log(np.abs(df['NI_n'][df['NI_n'] < 0]))
df = df.assign(log_RD=np.log(df['rnd']))

# calculate increase in revenue
df = df.assign(g=np.nan)
gb = df.groupby('ticker')
# create a new data frame to accommodate the new column g
df = pd.DataFrame([])
for group in gb:
    length = len(group[1].index)
    for i in range(1, length):
        group[1].at[group[1].index[i], 'g'] = group[1].at[group[1].index[i],
                                                          'revenue'] - group[1].at[group[1].index[i-1], 'revenue']
    df = df.append(group[1])

# only keep desired columns
df = df.drop(columns=['dimension', 'marketcap', 'netinc',
                      'assets', 'liabilities', 'rnd', 'revenue'])
print(df.columns)
