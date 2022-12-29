#!/bin/bash

hive -f create_db_yellow_taxi.sql
hive -f taxi_data.sql
hive -f trip.sql
hive -f trip_load.sql
hive -f dim_vendor.sql
hive -f dim_vendor_load.sql
hive -f rates.sql
hive -f rates_load.sql
hive -f payment.sql
hive -f payment_load.sql
hive -f taxi_mart.sql
hive -f taxi_mart_view.sql
hive -f taxi_mart_load.sql
