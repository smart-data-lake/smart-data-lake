---
title: Industrializing our data pipeline
---

Now that we have successfully loaded and analyzed the data, we show the results to our friend Tom.
He is very satisfied with the results and would like to bring this data pipeline to production. He is especially interested in keeping all historical data, in order to analyze data over time.
From your side you tell Tom, that the pipeline works well, but that CSV-Files are not a very stable and reliable data format, and that you'll suggest him a better solution for this. 

## Agenda

In Part 2 we will cover the following two points:
1. Using a better, transactional data format: Delta Lake
2. Keeping historical data: Historization and Deduplication

Additionally we will use spark-shell to interact with our data.

Part 2 is based on departures/airports/btl.conf of Part 1, see files ending with `part-1-solution` in [this directory](https://github.com/smart-data-lake/getting-started/tree/master/config).
Use the following cmd to reset your configuration files to the final solution of Part 1:

```
pushd config && cp departures.conf.part-1-solution departures.conf && cp airports.conf.part-1-solution airports.conf && cp btl.conf.part-1-solution btl.conf && popd
```
