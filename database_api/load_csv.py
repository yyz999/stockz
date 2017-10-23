import database_lib
import csv
import sys

if len(sys.argv) != 3:
    print('argv error. Use python3 script.py datafilepath datasource')
    exit()
file_name = sys.argv[1]
file_source = sys.argv[2]

# main
if file_source != "quandl":
    print('data source not supported')
    exit()
db_client = database_lib.StockzDatabaseClient()
with open(file_name) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        db_client.InsertStockData(
            row['ticker'], row['date'], open_p=row['open'], close_p=row['close'],
            high_p=row['high'], low_p=row['low'], volume_p=row['volume'],
            open_adj=row['adj_open'], close_adj=row['adj_close'],
            high_adj=row['adj_high'], low_adj=row['adj_low'],
            volume_adj=row['adj_volume'], ex_dividend=row['ex-dividend'],
            split_ratio=row['split_ratio'])
print("All done")
