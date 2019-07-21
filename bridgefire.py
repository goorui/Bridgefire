import quandl, datetime
import pandas as pd
import numpy as np
quandl.save_key('J3AKfEcoxgiV2TtMr2ue')
quandl.read_key()

# repeatedly get all data and convert to pandas data frame
df1 = pd.DataFrame([])
cursor_id = None
while True: # change to while True in working environment
    data = quandl.Datatable('SHARADAR/SF1').data(params = {'qopts':{'columns':['ticker', 'dimension', 'calendardate', 'netinc', 'marketcap', 'assets','liabilities', 'netinc', 'rnd', 'revenue'], 'cursor_id':cursor_id}})
    cursor_id = data.meta['next_cursor_id']
    df1 = df1.append(data.to_pandas())
    if cursor_id is None:
        break

# we only need annual data
df1 = df1[df1['dimension'] == 'MRY']
#print(df1.shape)

# sort the frame according to ticker and calendar date and data cleaning
df1 = df1.sort_values(by=['ticker', 'calendardate'])


df1 = df1.drop_duplicates(keep = 'last') # if there's duplicate statements, we only keep the latest one
    # after dropping duplicates, there're about 8748 rows

df1['rnd'] = df1['rnd'].where(df1['rnd'] > 0,1) # because we want to use logarithm afterwards, we replace all non-positive value with 1

# create new columns in the frame following the tutorial, we replace log_NC with NC
df1 = df1.assign(NC = df1['assets'] - df1['liabilities'])
df1 = df1.assign(LEV = df1['assets'] / df1['liabilities'])
df1 = df1.assign(log_mcap = np.log(df1['marketcap']))
df1 = df1.assign(log_mcap = np.log(df1['marketcap']))

df1['netinc'] = np.abs(df1['netinc'])
df1['netinc'] = df1['netinc'].where(df1['netinc'] > 0,1)
df1 = df1.assign(NI_p = np.log(np.abs(df1['netinc'])))

df1 = df1.assign(NI_n = df1['netinc'] + 1)
df1 = df1.assign(NI_n = np.log(np.abs(df1['NI_n'][df1['NI_n']<0])))
df1 = df1.assign(log_RD = np.log(df1['rnd']))

# calculate increase in revenue
df1 = df1.assign(g = np.nan)
gb = df1.groupby('ticker')
df1 = pd.DataFrame([]); # create a new data frame to accommodate the new column g
for group in gb:
    length = len(group[1].index)
    for i in range(1,length):
        group[1].at[group[1].index[i], 'g'] = group[1].at[group[1].index[i], 'revenue'] - group[1].at[group[1].index[i-1], 'revenue']
    df1 = df1.append(group[1])

# only keep desired columns
#df = df.drop(columns = ['dimension', 'marketcap', 'netinc', 'assets', 'liabilities', 'rnd', 'revenue'])

#print(df1.loc[df1['ticker'] == 'ZUMZ'])

data = quandl.Datatable('SHARADAR/TICKERS').data(params = {'qopts':{'columns':['ticker','siccode']}})
df = data.to_pandas()

sic_range_mins = [100,1000,1500,1000,2000,4000,5000,5200,6000,7000,9100,9900,10000]

sic_dict = {-1:"Error_low",
            -2:"Error_high",
            100:"Agriculture, Forestry and Fishing",
            1000:"Mining",
            1500:"construction",
            1000:"no used",
            2000:"Manufacturing",
            4000:"Transportation, Communications, Electric, Gas and Sanitary service",
            5000:"Wholesale Trade",
            5200:"Retail Trade",
            6000:"Finance, Insurance and Real Estate",
            7000:"Services",
            9100:"Publi Adminstration",
            9900:"Nonclassifiable"}

def sic_hash(sic):
    if sic< sic_range_mins[0]:
        return -1;
    for i in range(len(sic_range_mins)-1): # i goes from 0 to len-2
        if sic<sic_range_mins[i+1]:
            return sic_range_mins[i]
    return -2;


def test(s, model):
     if s == model:
        return 1
     return 0

df['sic_hash'] = df.apply(lambda row: sic_hash(row['siccode']),axis = 1) #creating a single column, in each row apply sic_hash to the siccode
df['industry'] = df.apply(lambda row: sic_dict[row['sic_hash']],axis = 1)

unique_sic_hashes = df['sic_hash'].unique() # change the column to an array of unique values

for sh in unique_sic_hashes:
    colname = "industry: "+ sic_dict[sh]
    df[colname] = df.apply(lambda row: test(row['sic_hash'],sh),axis=1)

#print(df.head())
print(df1.head())
df1 = df1.merge(df,how='inner',left_on = 'ticker',right_on = 'ticker')

#print(df1.head())
#print(df.loc[df['ticker'] == 'A'])
print(df1.head())

