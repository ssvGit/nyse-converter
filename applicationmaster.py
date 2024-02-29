from dask import dataframe as dd
import sys
import os
def convert_data():
    df = dd.read_csv(os.environ['SRC_FILE_PATTERN'],
                     names=["ticker","trade_date","open_price","low_price","high_price","close_price","volume"],
                     blocksize=None)
    df.to_json(os.environ['DESTIN_FILE_PATTERN'],orient='records',lines=True,compression='gzip',name_function=lambda i:'%05d' %i)


if __name__ == "__main__":
    print(convert_data())
