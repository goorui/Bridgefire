import quandl, datetime, pandas
import numpy as np

quandl.save_key('J3AKfEcoxgiV2TtMr2ue')
quandl.read_key()

# repeatedly get all data and convert to pandas data frame
data_list = []
cursor_id = None
while True:
    data = quandl.Datatable('SHARADAR/SF1').data(params = {'qopts':{'columns':['ticker', 'dimension', 'calendardate', 'netinc', 'marketcap', 'assets','liabilities', 'netinc', 'rnd', 'revenue']}})
    cursor_id = data.meta['next_cursor_id']
    data_list.append(data)
    if cursor_id is None:
        break
df = data_list.to_pandas()
print(df.shape)

# we only need annual data
df = df[df['dimension'] == 'MRY']
print(df.shape)

# sort the frame according to ticker and calendar date and data cleaning
df = df.sort_values(by=['ticker', 'calendardate'])


df = df.drop_duplicates(keep = 'last') # if there's duplicate statements, we only keep the latest one
    # after dropping duplicates, there're about 8748 rows

df['rnd'] = df['rnd'].replace(0,1) # because we want to use logarithm afterwards
df['rnd'] = df['rnd'].fillna(1)
df = df.drop(df[df.ticker == 'ZAAP'].index) # we drop ticker ZAAP since it has negative Research & Development


# create new columns in the frame following the tutorial, except for log_NC
df = df.assign(LEV = df['assets'] / df['liabilities'])
df = df.assign(log_mcap = np.log(df['marketcap']))
df = df.assign(NI_p = np.log(np.abs(df['netinc'])))
df = df.assign(NI_n = df['netinc'] + 1)
df = df.assign(NI_n = np.log(np.abs(df['NI_n'][df['NI_n']<0])))
df = df.assign(log_RD = np.log(df['rnd']))
df = df.assign(g = np.nan)

gb = df.groupby('ticker')

count = 0;
# for name, group in gb:
#     print(group)
#     count += 1
#     print(count)
