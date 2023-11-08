# Instructions
# View the row count of df1 and df2.
# Combine df1 and df2 in a new DataFrame named df3 with the union method.
# Save df3 to a parquet file named AA_DFW_ALL.parquet.
# Read the AA_DFW_ALL.parquet file and show the count.

# View the row count of df1 and df2
print("df1 Count: %d" % df1.count())
print("df2 Count: %d" % df2.count())

# Combine the DataFrames into one
df3 = df1.union(df2)

# Save the df3 DataFrame in Parquet format
df3.write.parquet('AA_DFW_ALL.parquet', mode='overwrite')

# Read the Parquet file into a new DataFrame and run a count
print(spark.read.parquet('AA_DFW_ALL.parquet').count())


# The Parquet format is a columnar data store, allowing Spark to use predicate pushdown. 
# This means Spark will only process the data necessary to complete the operations you define versus reading the entire dataset. 
# This gives Spark more flexibility in accessing the data and often drastically improves performance on large datasets.
