from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("ManufactureData").getOrCreate()
# checked the manf_data.csv data and observed the below points.
# found a malformed data available in the data, so planning to do Data Munging activity and use 'mode' as 'dropmalformed'
# header info was available, so planning to use 'header' as 'true'
# found two duplicate cols like prod_id & prodid and station_id twice, so planning to rename the cols with customized name using toDF
# Get the plain Data from the source
# Data Munging and cleaning process

basicdf = spark.read.csv("file:///home/hduser/sparkdata/manuf_data.csv",sep=",",header="true",inferSchema="true",mode="dropmalformed")\
          .toDF("timestamp","prod_id1","station_id1","sensor","value","station_id2","prod_id2","part_id")

# print(basicdf.count()) # uncomment this line, to verify the eliminated malformed data
# basicdf.show(8,truncate=False) # uncomment this line, to verify the data after the malformed data is removed

basicdf.createOrReplaceTempView("manf_view")

# Data Transformation Process

# Convert the timestamp column to regular Date col as recorded_date
# Merge the prod_id1 and prod_id2 to get product_id
# Merge the station_id1 and station_id2 to get station_id
# As we found 'status' data part of the sensor value, we are cleaning the sensor data by removing the status data
scrub_df = spark.sql("select CAST(timestamp AS DATE) as recorded_date, "
                     "NVL(prod_id1, prod_id2) as product_id, "
                     "NVL(station_id1, station_id2) as station_id, "
                     "CASE WHEN lower(sensor) = 'status' THEN '' else sensor END AS sensor,"
                     "value, part_id from manf_view")

# Data Enrichment Process

# As per requirement, cleaned data of the status, we need to make it as a column and hold the value of the status as value in the new column
# As the status column is added, the value of the status should be removed from its record too.
cur_df=scrub_df.withColumn("status", when(scrub_df.sensor != '', '').otherwise(scrub_df.value))\
       .withColumn("value",when(scrub_df.sensor == '', '').otherwise(scrub_df.value))

#cur_df.show(10,truncate=False) # uncomment this line, check for the transformed data

# mode overwrite and header true can be used based on the requirement
cur_df.write.mode("overwrite").csv("file:///home/hduser/sparkdata/manuf_transformed_data.csv",header=True)
