package com.bigdata.entities

case class StopTimes(trip_id: String,
                     arrival_time: String,
                     departure_time: String,
                     stop_id: String,
                     stop_sequence: Integer)
