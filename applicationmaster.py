from dask import dataframe as dd
import sys
import os
import glob

def convert_data():
    src_env =os.environ['SRC_FILE_PATTERN']
    src_files = glob.glob(f'{src_env}/NYSE*.txt.gz')
    # trgt_env = os.environ['TGT_FILE_PATTERN']
    tgt_files = [file.replace('txt','json') for file in src_files]
    df = dd.read_csv(src_files,
                     names=["ticker","trade_date","open_price","low_price","high_price","close_price","volume"],
                     blocksize=None)
    df.to_json(tgt_files,orient='records',lines=True,compression='gzip')
    

if __name__ == "__main__":
    print(convert_data())
