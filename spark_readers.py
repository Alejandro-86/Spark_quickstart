from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ReadDifferentFileFormats") \
    .getOrCreate()
import pyspark.sql.functions as F

def flatten_df(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']
    flat_df = nested_df.select(flat_cols + [F.col(nc+'.'+c).alias(nc+'_'+c) for nc in nested_cols for c in nested_df.select(nc+'.*').columns])
    return flat_df


# Read Parquet file
parquet_df = spark.read.parquet("datasets/flights.parquet")

# Read JSON file
json_df = spark.read.option("multiline", "true").json("datasets/debates.json")

# Read CSV file
csv_df = spark.read.csv("datasets/cars.csv", header=True, inferSchema=True)

# Read text file
text_df = spark.read.text("README.md")

# Print the first 5 rows of each dataframe
parquet_df.show(5)
json_df.show(5)
csv_df.show(5)
text_df.show(5)

# Count the number of rows in each dataframe
print("Number of rows in Parquet dataframe: ", parquet_df.count())
print("Number of rows in JSON dataframe: ", json_df.count())
print("Number of rows in CSV dataframe: ", csv_df.count())
print("Number of rows in Text dataframe: ", text_df.count())

# Common operations on the dataframes
# Filter the dataframe

# Filter the dataframe to get the flights with distance greater than 1000
print("Flights with distance greater than 1000 miles: ")
parquet_df.filter(parquet_df.DISTANCE > 1000).show(5)

# Select the columns
print("Selecting the columns from the dataframe: ")
csv_df.select("model", "disp", "drat", "carb").show(5)

# Fix json dataframe to get the nested columns
# Flatten the nested columns
print("Flattening the nested columns in the JSON dataframe: ")
json_df = flatten_df(json_df)
json_df.show(5)

# windowing function
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
print("Adding a row number to the dataframe: ")
windowSpec = Window.partitionBy(csv_df['cyl']).orderBy(csv_df['model'])
csv_df = csv_df.withColumn("row_number", row_number().over(windowSpec))
csv_df.show()


# explode the array column
print("Exploding the array column in the JSON dataframe: ")
print("Count before exploding: ", json_df.count())
json_df = json_df.withColumn("results", F.explode("results"))
print("Count after exploding: ", json_df.count())

# Grouping, aggregation and ordering
print("Count of cars by horsepower: ")
csv_df.groupBy("hp").agg({"hp":"count"}).orderBy("hp", ascending=False).show(5)

# Joining the dataframes
print(f"Count of flights: {parquet_df.count()}")
less_than_300 = parquet_df.filter(parquet_df.AIR_TIME <= 300)
more_than_300 = parquet_df.filter(parquet_df.AIR_TIME > 300)
print(f"Count of flights with AIRTIME longer than 300: {less_than_300.count()}")
print(f"Count of flights with AIRTIME less than 300: {more_than_300.count()}")
# Join the dataframes
joined_df = less_than_300.join(more_than_300, "FL_DATE", "inner")
print(f"Count joined_df: {joined_df.count()}")
# Stop Spark session
spark.stop()