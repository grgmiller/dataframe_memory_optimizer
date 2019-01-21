# Developed by Greg Miller, based on code written by Josh Devlin (@jaypeedevlin) at https://www.dataquest.io/blog/pandas-big-data/
# Last updated January 21, 2019
# Version 1.0

#Purpose: To return a dictionary of dtypes for each column of a large csv file that  
#         are optimized reduce the memory size of the dataframe that is created when using read_csv()

import csv
import numpy as np
import pandas as pd

'''
Reference information about dtypes 
               max # of values         value range          unsigned value range                Available dtypes
dtype8         256      (2^8)          -128 to 127          0 to 255                            int
dtype16        65,536   (2^16)         -32768 to 32767      0 to 65,535                         int, float
dtype32        ~4.3 B   (2^32)         -2.1B to 2.1B        0 to 4,294,967,295                  int, float
dtype64        1.8E19   (2^64)         -9E18 to 9E18        0 to 18,446,744,073,709,551,615     int, float, datetime
'''

dataFile = input(' Enter the name of the .csv file in the current working directory that you wish to optimize (e.g. "filename.csv") >')
while True:
    try:
        df = pd.read_csv(dataFile)
        break
    except:
        dataFile = input('\n ERROR: The .csv file name you entered was not found. Please re-enter the filename as "filename.csv" > ')

def main():
    avg_mem_by_dtype() #use this to find average memory by data type
    optimized_df = downsize_dytpes() #use this to downsize all int and float dtypes
    dtype_dict(optimized_df) #use this to generate a dictionary that you can use as the dtype option in csv_read to optimize dataframe upon load.

def avg_mem_by_dtype(): #print average memory use for each dtype
    print('\nOrginal dtypes and memory use:')
    for dtype in ['float64','int64','object','bool','datetime64','timedelta']:
        selected_dtype = df.select_dtypes(include=[dtype])
        ncol = selected_dtype.shape[1]
        mean_usage_b = selected_dtype.memory_usage(deep=True).mean()
        mean_usage_mb = mean_usage_b / 1024 ** 2
        print("Avg memory use for {} {} columns: {:03.2f} MB".format(ncol,dtype,mean_usage_mb))

def mem_usage(pandas_obj):
    if isinstance(pandas_obj,pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else: # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2 # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)

def mem_percent(original, optimized):
    if isinstance(original,pd.DataFrame):
        usage_b_orig = original.memory_usage(deep=True).sum()
        usage_b_opt = optimized.memory_usage(deep=True).sum()
    else: # we assume if not a df it's a series
        usage_b_orig = original.memory_usage(deep=True)
        usage_b_opt = optimized.memory_usage(deep=True)
    percent = (usage_b_orig-usage_b_opt)/usage_b_orig*100
    return "{:03.1f} %".format(percent)

def downsize_dytpes(): #use this to downsize all int and float dtypes
    #convert int dtypes
    df_int = df.select_dtypes(include=['int64'])
    converted_int = df_int.apply(pd.to_numeric,downcast='integer')
    converted_int = converted_int.apply(pd.to_numeric,downcast='unsigned')

    compare_ints = pd.concat([df_int.dtypes,converted_int.dtypes],axis=1)
    compare_ints.columns = ['Before','After']
    print('\nInteger downsizing results:')
    print(compare_ints)
    print('Original int size:  '+mem_usage(df_int))
    print('Optimized int size: '+mem_usage(converted_int))
    print('Percent reduction:  '+mem_percent(df_int, converted_int))

    #convert float dtypes
    df_float = df.select_dtypes(include=['float64'])
    converted_float = df_float.apply(pd.to_numeric,downcast='float')

    compare_floats = pd.concat([df_float.dtypes,converted_float.dtypes],axis=1)
    compare_floats.columns = ['before','after']
    print('\nFloat downsizing results:')
    print(compare_floats)
    print('Original float size:  '+mem_usage(df_float))
    print('Optimized float size: '+mem_usage(converted_float))
    print('Percent reduction:    '+mem_percent(df_float, converted_float))

    #convert object dtypes
    df_obj = df.select_dtypes(include=['object'])
    converted_obj = pd.DataFrame()
    print('\nObject downsizing results:')
    for col in df_obj.columns:
        num_unique_values = len(df_obj[col].unique())
        num_total_values = len(df_obj[col])
        ratio = round(num_unique_values / num_total_values * 100, 2)
        if num_unique_values / num_total_values < 0.5:
            print('Column "{}" has {} % (n={}) unique values, and was converted to a categorical'.format(col,ratio,num_unique_values))
            converted_obj.loc[:,col] = df_obj[col].astype('category')
        else:
            print('Column "{}" has {} % (n={}) unique values, and was left as an object datatype'.format(col,ratio,num_unique_values))
            converted_obj.loc[:,col] = df_obj[col]
    print('Original object size:  '+mem_usage(df_obj))
    print('Optimized object size: '+mem_usage(converted_obj))
    print('Percent reduction:     '+mem_percent(df_obj, converted_obj))

    optimized_df = df.copy()
    optimized_df[converted_int.columns] = converted_int
    optimized_df[converted_float.columns] = converted_float
    optimized_df[converted_obj.columns] = converted_obj
    
    print('\nOriginal dataframe size:   '+mem_usage(df))
    print('Optimized dataframe size:  '+mem_usage(optimized_df))
    print('Percent reduction in size: '+mem_percent(df, optimized_df))
    return optimized_df

def dtype_dict(optimized_df): #use this to generate a dictionary that you can use as the dtype option in csv_read to optimize dataframe upon load.
    dtypes = optimized_df.dtypes
    dtypes_col = dtypes.index
    dtypes_type = [i.name for i in dtypes.values]
    column_types = dict(zip(dtypes_col, dtypes_type))
    optimized_read = pd.read_csv(dataFile,dtype=column_types)
    print('\nAdd the following global variable to your code, and use pd.read_csv("filename.csv", dtypes=df_dtypes) when reading the file:')
    print('\ndf_dtypes =', column_types)
    print('\n')

#Other useful, but unused memory functions
def df_info(): #print dtypes and total filesize
    print('Current dataframe memory usage:')
    df.info(memory_usage='deep')

def min_max(): #use this to figure out the minimum and maximum values so you can look for downsizing and unsigned opportunities
    print('\n MAXIMUM VALUES:')
    print(df.max(numeric_only=True))
    print('\n MINIMUM VALUES:')
    print(df.min(numeric_only=True))

if __name__== "__main__":
    main()