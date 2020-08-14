import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import concurrent.futures
from tqdm import tqdm
import os
import json
import warnings
import threading
from concurrent.futures import ThreadPoolExecutor
import threading
import random
import boto3
import pandas as pd
import io
import time
import s3fs
import multiprocessing
import time
 
print('Loading function')
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("--ncpu", help="total process needed", type=int, default=20)
parser.add_argument("--s3-in-bucket", help="the bucket of input files", type=str, default='##')
parser.add_argument("--s3-in-dir", help="the directory of output files", type=str, default='###/')
parser.add_argument("--s3-out-bucket", help="the bucket of output files", type=str, default='###')
parser.add_argument("--s3-out-dir", help="the directory of output files", type=str, default='###')
args = parser.parse_args()
ncpu=args.ncpu
dirct=args.s3_out_dir
in_bucket=args.s3_in_bucket
out_bucket=args.s3_out_bucket
in_dirct=args.s3_in_dir
 
###step 1: print all file names in bucket
client=boto3.client('s3')
def get_s3_keys(bucket,prefix):
   resp=client.list_objects_v2(Bucket=bucket,Prefix=prefix)
   all_files=[]
   for obj in resp['Contents']:
       this_file=obj['Key']
       if not this_file.endswith('/'):
           all_files.append(this_file)
   return all_files
 
###step 2: now read the csv file
def get_output(file_dir):
   obj = client.get_object(Bucket=in_bucket, Key=file_dir)
   grid_sizes = pd.read_csv(obj['Body'])
   return grid_sizes
 
fs = s3fs.S3FileSystem()
bucket_name = out_bucket
directory_name =  dirct
 
def write_to_file(input_df, idx):
   idx=str(idx)
   bytes_to_write = input_df.to_csv(None, index=False).encode()
   with fs.open('s3://' + bucket_name + '/' + directory_name + '/' + idx + '/' + idx + '.csv', 'wb') as f:
       f.write(bytes_to_write)
   return
def run_process(task):
   idx = task
   new_df = df[df.pointid == idx].sort_values(by=['timestamp'])
   write_to_file(new_df, idx)
   return True
 
def parallel_process(array, function, n_jobs=ncpu):
   # If we set n_jobs to 1, just run a list comprehension. This is useful for benchmarking and debugging.
   if n_jobs == 1:
       result = []
       futures = [function(idx) for idx in (array)]
       return result
   # Assemble the workers
   with multiprocessing.Pool(n_jobs) as pool:
       pool.map(function, array)
       # Print out the progress as tasks complete
   pool.close()
   pool.join()
   print("Done")
   return
 
filename=get_s3_keys(in_bucket,in_dirct)###different file folders
df=pd.DataFrame()
print(type(df))
for file_index in filename:
   file_dir=file_index
   df_current=get_output(file_dir) ###currently  this step is down
   df_current=df_current.iloc[:,0:3]
   df_current.columns=['pointid','timestamp','value']
   df=df.append(df_current,ignore_index=True)
print(type(df))
point_ids = list(set(df.pointid))    ###step 3: now write CSV to file
parallel_process(point_ids, run_process, n_jobs=ncpu)
