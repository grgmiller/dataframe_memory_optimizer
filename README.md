# dataframe_memory_optimizer

The purpose of this code is to reduce the size (memory use) of dataframes that are created when using Pandas.read_csv().

It accomplishes this by determining the smallest datatype that can be used for each column based on the data it contains, and then returning a dictionary matching column names to datatypes that you can copy into your code in order to automatically use those datatypes when reading the csv.

For example, this script might return the following output:
```
df_dtypes = {'col1': 'category', 'col2': 'uint8', 'col3': 'float32'}
```
Which, if you copy into your code can be used like so when reading a csv file:
```
pd.read_csv('filename.csv', dtype=df_dtypes)
```

Most of the code snippets I adapted were written by Josh Devlin ([@jaypeedevlin](https://github.com/jaypeedevlin)) on https://www.dataquest.io/blog/pandas-big-data/
