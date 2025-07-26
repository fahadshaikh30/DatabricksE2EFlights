import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Bookings Data
# Creating a streaming table using dlt. This will load the table incrementally and perform no transformations since its on the staging environment.
@dlt.table(
  name="stage_bookings"
  )
def stage_bookings():
  df = spark.readStream.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
  return df

# Creating a streaming view and performing transformations on the data
@dlt.view(
    name="trans_bookings"
)
def trans_bookings():
  df = dlt.readStream("stage_bookings")
  df = df.withColumn("amount", col("amount").cast(DoubleType()))\
    .withColumn("modifiedDate", current_timestamp())\
    .withColumn("booking_date", to_date(col("booking_date")))\
    .drop("_rescued_data")
  return df

# These are the rules that are defined for the silver table
rules = {
    "rule1" : "booking_id IS NOT NULL",
    "rule2" : "passenger_id IS NOT NULL",
    # "rule3" : "flight_id IS NOT NULL",
    # "rule4" : "airport_id IS NOT NULL"
}

# Creating a streaming table using dlt. This will load the table to a steaming silver table.
@dlt.table(
    name="silver_bookings"
)
@dlt.expect_all(rules)
def silver_bookings():
  df = dlt.readStream("trans_bookings")
  return df

# Flights Data
@dlt.view(
    name="trans_flights"
)
def trans_flights():
  df = spark.readStream.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")
  df = df.withColumn("modifiedDate", current_timestamp())\
    .drop("_rescued_data")
  return df

dlt.create_streaming_table(
    name="silver_flights",
    comment="This is the silver table for flights")

dlt.create_auto_cdc_flow(
  target = "silver_flights",
  source = "trans_flights",
  keys = ["flight_id"],
  sequence_by = col("modifiedDate"),
  stored_as_scd_type = 1,
)

# Passengers Data
@dlt.view(
    name="trans_passengers"
)
def trans_passengers():
  df = spark.readStream.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/passengers/data/")
  df = df.withColumn("modifiedDate", current_timestamp())\
    .drop("_rescued_data")
  return df

dlt.create_streaming_table(
    name="silver_passengers",
    comment="This is the silver table for passengers")

dlt.create_auto_cdc_flow(
  target = "silver_passengers",
  source = "trans_passengers",
  keys = ["passenger_id"],
  sequence_by = col("modifiedDate"),
  stored_as_scd_type = 1,
)

# Airports Data
@dlt.view(
    name="trans_airports"
)
def trans_airports():
  df = spark.readStream.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
  df = df.withColumn("modifiedDate", current_timestamp())\
    .drop("_rescued_data")
  return df

dlt.create_streaming_table(
    name="silver_airports",
    comment="This is the silver table for airports")

dlt.create_auto_cdc_flow(
  target = "silver_airports",
  source = "trans_airports",
  keys = ["airport_id"],
  sequence_by = col("modifiedDate"),
  stored_as_scd_type = 1,
)

# Silver Business View
# Join all the tables together
@dlt.table(
    name="silver_business"
)
def silver_business():
  df = dlt.readStream("silver_bookings")\
    .join(dlt.readStream("silver_flights"), ["flight_id"])\
    .join(dlt.readStream("silver_passengers"), ["passenger_id"])\
    .join(dlt.readStream("silver_airports"), ["airport_id"])\
    .drop("modifiedDate")
  return df

# Materialized View
@dlt.table(
    name="silver_business_mat"
)
def silver_business_mat():
  df = dlt.read("silver_business") # read for mat view instead of readStream
  return df