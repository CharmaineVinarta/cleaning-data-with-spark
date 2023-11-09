# Here, The manager asked to split the VOTER_NAME column into words on any space character. 
# Treat the last word as the last_name, and all other words as the first_name. 
# Using some new functions in this exercise including .split(), .size(), and .getItem(). 
# The .getItem(index) takes an integer value to return the appropriately numbered item in the column. 
# The functions .split() and .size() are in the pyspark.sql.functions library.

# The filtered voter DataFrame from your previous exercise is available as voter_df. The pyspark.sql.functions library is available under the alias F.

# Instructions
# Add a new column called splits holding the list of possible names.
# Use the getItem() method and create a new column called first_name.
# Get the last entry of the splits list and create a column called last_name.
# Drop the splits column and show the new voter_df.

# Add a new column called splits separated on whitespace
voter_df = voter_df.withColumn('splits', F.split(voter_df.VOTER_NAME, '\s+'))

# Create a new column called first_name based on the first item in splits
voter_df = voter_df.withColumn('first_name', voter_df.splits.getItem(0))

# Get the last entry of the splits list and create a column called last_name
voter_df = voter_df.withColumn('last_name', voter_df.splits.getItem(F.size('splits') - 1))

# Drop the splits column
voter_df = voter_df.drop('splits')

# Show the voter_df DataFrame
voter_df.show()
