from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType
from pyspark.sql.functions import when, col, upper, lit

spark = SparkSession.builder.master("local[*]").appName("ManufactureData").getOrCreate()

# checked the manf_data.csv data and observed the below points.
# found a malformed data available in the data, so planning to do Data Munging activity and use 'mode' as 'dropmalformed'
# header info was available, so planning to use 'header' as 'true'
# found two duplicate cols like prod_id & prodid and station_id twice, so planning to rename the cols with customized name using toDF
# Get the plain Data from the source

# Data Munging and cleaning process

manf_schema = StructType([StructField("timestamp",DateType(),False),
                          StructField("prod_id1",StringType(),False),
                          StructField("station_id1",IntegerType(),False),
                          StructField("sensor",StringType(),True),
                          StructField("value",StringType(),True),
                          StructField("station_id2",IntegerType(),False),
                          StructField("prod_id2",StringType(),False),
                          StructField("part_id",StringType(),False)])

basicdf = spark.read.csv("file:///home/hduser/sparkdata/manuf_data.csv",sep=",",header="true",schema=manf_schema,mode="dropmalformed")\
          .toDF("timestamp","prod_id1","station_id1","sensor","value","station_id2","prod_id2","part_id")

#print(basicdf.count()) # uncomment this line, to verify the eliminated malformed data
#basicdf.show(2520,truncate=False) # uncomment this line, to verify the data after the malformed data is removed

# Data Transformation Process includes validation, enrichment and scrubbing of the data

# As we used the DateType in the schema, the recorded_date will hold the date without timestamp
# Merge the prod_id1 and prod_id2 to get product_id
# Merge the station_id1 and station_id2 to get station_id
# As we found 'status' data part of the sensor value, we are cleaning the sensor data by removing the status data
manuftransdf = basicdf.withColumn("prod_id", when(col("prod_id1") != '', col("prod_id1")).otherwise(col("prod_id1"))).\
               withColumn("station_id", when(col("station_id1") != '', col("station_id1")).otherwise(col("station_id2"))).\
               withColumn("sensor", when(upper(col("sensor")) == 'STATUS', '').otherwise(col("sensor"))).\
               withColumn("value", when(upper(col("value")) == 'OK', '').otherwise(col("value"))).\
               withColumn("status", when(col("sensor") == '', 'OK').otherwise(lit("")))

#manuftransdf.show(15,truncate=False) # uncomment this line, check for the transformed data

# Data cleaning process of removal of unwanted col data

cleanmanufdf = manuftransdf.select(col("timestamp").alias("recorded_date"), "prod_id", "station_id", "sensor", "value", "part_id", "status").\
               drop("prod_id1", "prod_id2", "station_id1", "station_id2")

#cleanmanufdf.show(15, truncate=False); # uncomment this line to verify the removed columns

# Data stored into file system in csv format

#mode overwrite and header true can be used based on the requirement
cleanmanufdf.write.mode("overwrite").csv("file:///home/hduser/sparkdata/manuf_transformed_dsl_data.csv",header=True)
