from dask import dataframe as dd
import sys
def convert_data():
    df = dd.read_csv('data-master/nyse_all/nyse_data/NYSE_*.txt.gz',
                     names=["ticker","trade_date","open_price","low_price","high_price","close_price","volume"],
                     blocksize=None)
    df.to_json('data-master/nyse_all/nyse_json/NYSE_*.json.gz',orient='records',lines=True,compression='gzip',name_function=lambda i:'%05d' %i)


if __name__ == "__main__":
    print(convert_data())
