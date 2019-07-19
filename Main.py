import quandl, datetime, pandas
import numpy as np

quandl.save_key('J3AKfEcoxgiV2TtMr2ue')
quandl.read_key()

# get data and convert to pandas data frame
data = quandl.Datatable('SHARADAR/SF1').data(params = {'qopts':{'columns':['ticker', 'calendardate', 'netinc', 'marketcap', 'assets','liabilities', 'netinc', 'rnd', 'revenue']}})
df = data.to_pandas()

# sort the frame according to ticker and calendar date and data cleaning
df = df.sort_values(by=['ticker', 'calendardate'])

df['rnd'] = df['rnd'].replace(0,1) # because we want to use logarithm afterwards
df['rnd'] = df['rnd'].fillna(1)
print(df[df['rnd'] < 0])
df = df.drop(df[df.ticker == 'ZAAP'].index) # we drop ticker ZAAP since it has negative Research & Development

# create new columns in the frame following the tutorial, except for log_NC
df = df.assign(LEV = df['assets'] / df['liabilities'])
df = df.assign(log_mcap = np.log(df['marketcap']))
df = df.assign(NI_p = np.log(np.abs(df['netinc'])))
df = df.assign(NI_n = df['netinc'] + 1)
df = df.assign(NI_n = np.log(np.abs(df['NI_n'][df['NI_n']<0])))
df = df.assign(log_RD = np.log(df['rnd']))


# print(df.columns)