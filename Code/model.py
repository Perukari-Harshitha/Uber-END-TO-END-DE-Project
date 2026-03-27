from pyspark import pipelines as dp

# create dim passenger table

@dp.view
def dim_passenger_view():
  df = spark.readStream.table("uber_db.bronze.silver_obt")
  df = df.select("passenger_name","passenger_id", "passenger_phone", "passenger_email")
  df = df.drop_duplicates(subset=['passenger_id'])
  return df

dp.create_streaming_table("dim_passenger")

dp.create_auto_cdc_flow(
  target = "dim_passenger",
  source = "dim_passenger_view",
  keys = ["passenger_id"],
  sequence_by = "passenger_id",
  stored_as_scd_type = 1
)

# create dim driver table

@dp.view
def dim_driver_view():
  df = spark.readStream.table("uber_db.bronze.silver_obt")
  df = df.select("driver_id", "driver_name", "driver_phone", "driver_rating", "driver_license")
  df = df.drop_duplicates(subset=['driver_id'])
  return df

dp.create_streaming_table("dim_driver")

dp.create_auto_cdc_flow(
  target = "dim_driver",
  source = "dim_driver_view",
  keys = ["driver_id"],
  sequence_by = "driver_id",
  stored_as_scd_type = 1
)

# create dim vehicle table

@dp.view
def dim_vehicle_view():
  df = spark.readStream.table("uber_db.bronze.silver_obt")
  df = df.select("vehicle_id", "vehicle_make_id", "vehicle_model", "vehicle_color", "vehicle_type")
  df = df.drop_duplicates(subset=['vehicle_id'])
  return df

dp.create_streaming_table("dim_vehicle")

dp.create_auto_cdc_flow(
  target = "dim_vehicle",
  source = "dim_vehicle_view",
  keys = ["vehicle_id"],
  sequence_by = "vehicle_id",
  stored_as_scd_type = 1
)

# create dim booking table

@dp.view
def dim_booking_view():
  df = spark.readStream.table("uber_db.bronze.silver_obt")
  df = df.select("ride_id", "confirmation_number", "dropoff_longitude", "ride_status", "ride_status_id", "dropoff_city_id", "cancellation_reason_id", "dropoff_address", "booking_timestamp", "dropoff_timestamp", "pickup_timestamp", "pickup_address", "pickup_latitude", "pickup_longitude")
  df = df.drop_duplicates(subset=['ride_id'])
  return df

dp.create_streaming_table("dim_booking")

dp.create_auto_cdc_flow(
  target = "dim_booking",
  source = "dim_booking_view",
  keys = ["ride_id"],
  sequence_by = "ride_id",
  stored_as_scd_type = 1
)

# create dim payment table

@dp.view
def dim_payment_view():
  df = spark.readStream.table("uber_db.bronze.silver_obt")
  df = df.select("payment_method_id", "payment_method", "is_card", "requires_auth")
  df = df.drop_duplicates(subset=['payment_method_id'])
  return df

dp.create_streaming_table("dim_payment")

dp.create_auto_cdc_flow(
  target = "dim_payment",
  source = "dim_payment_view",
  keys = ["payment_method_id"],
  sequence_by = "payment_method_id",
  stored_as_scd_type = 1
)

# create dim location table

@dp.view
def dim_location_view():
  df = spark.readStream.table("uber_db.bronze.silver_obt")
  df = df.select("state", "region", "pickup_city_id", "pickup_city")
  df = df.drop_duplicates(subset=['pickup_city_id'])
  return df

dp.create_streaming_table("dim_location")

dp.create_auto_cdc_flow(
  target = "dim_location",
  source = "dim_location_view",
  keys = ["pickup_city_id"],
  sequence_by = "pickup_city_id",
  stored_as_scd_type = 1
)

# create fact table

@dp.view
def fact_view():
  df = spark.readStream.table("uber_db.bronze.silver_obt")
  df = spark.readStream.table("uber_db.bronze.silver_obt")
  df = df.select("ride_id","pickup_city_id","payment_method_id","driver_id","passenger_id","vehicle_id","base_fare","distance_fare","base_rate","distance_miles","duration_minutes","surge_multiplier","subtotal","tip_amount","per_mile","rating","per_minute")
  return df


dp.create_streaming_table("fact")

dp.create_auto_cdc_flow(
  target = "fact",
  source = "fact_view",
  keys = ["ride_id","pickup_city_id","payment_method_id","driver_id","passenger_id","vehicle_id"],
  sequence_by = "ride_id",
  stored_as_scd_type = 1
)





