# Instructions
# Import the AA_DFW_ALL.parquet file into flights_df.
# Use the createOrReplaceTempView method to alias the flights table.
# Run a Spark SQL query against the flights table.

# Read the Parquet file into flights_df
flights_df = spark.read.parquet('AA_DFW_ALL.parquet')

# Register the temp table
flights_df.createOrReplaceTempView('flights')

# Run a SQL query of the average flight duration
avg_duration = spark.sql('SELECT avg(flight_duration) from flights').collect()[0]
print('The average flight time is: %d' % avg_duration)
