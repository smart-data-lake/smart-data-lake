
# Table with computed distances
The data is derived from [Airport locations](#/dataObjects/int-airports) and flight information


# Description
This business table porvides data about flight distances.
Further, flights with a travel distance <500km are tagged. 

The goal is to identify the amount of flight and all the locations, which are closer than 500km and therewith may be replaced by train travels. 

![less flights more ...](#/dataObjects/train.png)

# Properties

@column `distance` The computed distance between the departure and arrival airports. The distance is computed as length of on an sphere with radius 6371km using the geographic latitutes and longitudes of both airports.

@column `could_be_done_by_rail` Boolean describing if the distance is less than 500km (than true)

# Structure

This table is for decision making process and therewith part of the business transformation layer. 

In general we distinguish

| layer | usage |
|-------|-------|
| stage layer | copy of the original data, accessible for merging/combining with existing/other data sources |
| integration layer | cleaning/structuring/prepared data |
| business transformation layer | ready for data analysts/scientists to run/develop their applications |
