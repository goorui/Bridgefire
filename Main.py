import quandl
quandl.read_key()
data = quandl.Datatable('SHARADAR/SF1').data(params={'ticker': 'AAPL'}, qopts={'columns':['ticker', 'per_end_date']})
print(data.to_pandas())