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

Additionally we will use Polynote-Notebook to easily interact with our data.

Part 2 is based on [this](../config-examples/application-part1-compute-final.conf) configuration file, **copy it to config/application.conf** to walk through the tutorial.
