CREATE OR REFRESH STREAMING TABLE silver_obt
AS 

  select
    
       stage_rides.ride_id, stage_rides.confirmation_number, stage_rides.booking_timestamp, stage_rides.base_fare, stage_rides.cancellation_reason_id, stage_rides.distance_fare, stage_rides.distance_miles, stage_rides.duration_minutes, stage_rides.driver_id, stage_rides.driver_license, stage_rides.driver_name, stage_rides.driver_phone, stage_rides.driver_rating, stage_rides.passenger_id, stage_rides.vehicle_id, stage_rides.vehicle_make_id, stage_rides.payment_method_id, stage_rides.ride_status_id, stage_rides.pickup_city_id, stage_rides.dropoff_city_id, stage_rides.passenger_name, stage_rides.passenger_email, stage_rides.passenger_phone, stage_rides.vehicle_model, stage_rides.vehicle_color, stage_rides.license_plate, stage_rides.pickup_address, stage_rides.pickup_latitude, stage_rides.pickup_longitude, stage_rides.dropoff_address, stage_rides.dropoff_longitude, stage_rides.pickup_timestamp, stage_rides.dropoff_timestamp, stage_rides.surge_multiplier, stage_rides.subtotal, stage_rides.tip_amount, stage_rides.rating
           
                ,
           

    
       map_vehicle_makes.vehicle_make
           
                ,
           

    
       map_vehicle_types.vehicle_type,map_vehicle_types.description,map_vehicle_types.base_rate,map_vehicle_types.per_mile,map_vehicle_types.per_minute

                ,

      map_payment_methods.payment_method, map_payment_methods.is_card,map_payment_methods.requires_auth

                ,
           
    
       map_cities.city as pickup_city,map_cities.state, map_cities.region
           
                ,
           

    
       map_cancellation_reasons.cancellation_reason

                ,
           

    
       map_ride_statuses.ride_status,map_ride_statuses.is_completed
           

    
  FROM
    
        
            stream (uber_db.bronze.stage_rides) 
            WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES stage_rides
        
    
        
            LEFT JOIN uber_db.bronze.map_vehicle_makes map_vehicle_makes ON stage_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id
        
        
            LEFT JOIN uber_db.bronze.map_vehicle_types map_vehicle_types ON stage_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id

            LEFT JOIN uber_db.bronze.map_payment_methods map_payment_methods ON stage_rides.payment_method_id = map_payment_methods.payment_method_id

            LEFT JOIN uber_db.bronze.map_cities map_cities ON stage_rides.pickup_city_id = map_cities.city_id

            LEFT JOIN uber_db.bronze.map_cancellation_reasons map_cancellation_reasons ON stage_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id

          
            LEFT JOIN uber_db.bronze.map_ride_statuses map_ride_statuses ON stage_rides.ride_status_id = map_ride_statuses.ride_status_id

            