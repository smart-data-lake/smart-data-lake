---
title: Get Departure Coordinates
---

## Goal

In this step we will extend the [configuration file](../config-examples/application-compute-part1-join.conf) of the previous step
so that we get the coordinates and the readable name of Bern Airport in our final data.
Since we are dealing with just one record, we could manually add it to the data set.
But what if we wanted to extend our project to other departure airports in the future?
We'll do it in a generic way by adding another transformer into the action *join_departures_airports*

## Define join_departures_airports action

Let's start in an unusual way by first changing the action. You'll see why shortly.

      join-departures-airports {
        type = CustomSparkAction
        inputIds = [stg-departures, int-airports]
        outputIds = [btl-departures-arrivals-airports]
        transformers = [{
          type = SQLDfsTransformer
          code = {
            btl-connected-airports = """
              select stg_departures.estdepartureairport, stg_departures.estarrivalairport, 
                airports.*
              from stg_departures join int_airports airports on stg_departures.estArrivalAirport = airports.ident
            """
          }},
        {
          type = SQLDfsTransformer
          code = {
            btl-departures-arrivals-airports = """
              select btl_connected_airports.estdepartureairport, btl_connected_airports.estarrivalairport,
                btl_connected_airports.name as arr_name, btl_connected_airports.latitude_deg as arr_latitude_deg, btl_connected_airports.longitude_deg as arr_longitude_deg,
                airports.name as dep_name, airports.latitude_deg as dep_latitude_deg, airports.longitude_deg as dep_longitude_deg
              from btl_connected_airports join int_airports airports on btl_connected_airports.estdepartureairport = airports.ident
            """
          }
        }    
        ]
        metadata {
          feed = compute
        }
      }

We added a second transformer of the type SQLDfsTransformer.
It's SQL Code references the result of the first transformer: *btl-connected-airports* (remember the underscores, so *btl_connected_airports* in SparkSQL).
SDL will execute these transformations in the order you defined them, which allows you to chain them together, like we have done.

In the second SQL-Code, we join the result of the first SQL again with int_airports, but this time using *estdepartureairport* as key
to get the name and coordinates of the departures airport, Bern Airport.
We also renamed these columns so that they are distinguishable from the names and coordinates of the arrival airports.
Finally, we put the result into a DataObject called *btl-departures-arrivals-airports*.

## Define output object

Let's add the new DataObject, as usual:

      btl-departures-arrivals-airports {
        type = CsvFileDataObject
        path = "~{id}"
      }

Now we can simply delete the DataObject btl-connected-airports, because it is now only a temporary result within an action.
This is a key difference between chaining actions and chaining transformations within the same action:
you don't have intermediary results. 
Another difference is that you cannot run an individual transformation alone, you can only run entire actions.


## Try it out

[This](../config-examples/application-compute-part1-dep-arr.conf) is how your config should look like by now.

When running the example, you should see a CSV file with departure and arrival airport names and coordinates.

Great! Now we have all the data we need in one place. The only thing left to do is to compute the distance
between departure and arrival coordinates. Let's do that in the final step of part 1.
