---
title: Inputs
---

## Goal

Let's say your friend Tom is a fan of railways, and he lives next to an airport.
He wonders how many flights that start from his neighborhood could be replaced by rail traffic.
For that he would need to find out which flights depart from his airport, as well as how far they are flying.
If they fly less than, say 500 km, then that would be a journey that could be done by rail.

You just discovered this tool called Smart Data Lake Builder that's supposedly good for combining data from different sources and performing some analysis on it.
So you decide to help Tom by trying that framework.


## Our Input Data
Our first step is to get the input data.
After browsing the web a bit, you end up finding a website that looks promising.

### departures

The site is called [openskynetwork](https://openskynetwork.github.io/opensky-api/rest.html#id17),
and it provides you with a free REST-Interface for getting departures by airport.
Notice that you need the ICAO identifier of Tom's airport to get the right parameters.
You know that Tom lives near Bern, Switzerland. A quick web search shows you that the identifier is
*LSZB*. Let's focus on some specific time period for now to have reproducible results.
You end up with the following REST-URL:

    https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1696854853&end=1697027653

When you run this in your web-browser, you will get a response in the JSON Format.
For each record, it contains the ICAO identifier of the airport where the plane is flying to in the field
*estArrivalAirport*. That's a good start! 

:::info
Notice that the result of this JSON-call is exactly what was downloaded in the previous step into 
the folder *data/stg_departures/result.json*.
:::

### airports.csv
Now you need some kind of list of all airports with their respective locations.
You end up finding a [website](https://ourairports.com/data/) that has just that!
It hosts a csv-file called *airports.csv* which contains what you need.

:::info
Notice that this CSV-File is exactly what was downloaded in the previous step into
the folder *data/stg_airports/result.csv*.
:::

## Next step

Now that we know our input data, we can start our analysis.
In the next step, we will start *Part 1 of the Getting Started Guide* 
to do our first steps with Smart Data Lake Builder.


