# Dataset utilities

Some misc scripts for working with demo QuestDB datasets

## qdb_table_downloaded

Downloads the passed query, paginating every 1000000 million rows, and outputs a csv

## tfl_dataset

Downloads data from [TFL's OpenApi](https://api-portal.tfl.gov.uk/api-details#api=Mode&operation=Mode_Arrivals) and stores it on QuestDB. You can pass the transport "mode", for example _tube_ or _bus_ as seen at the [TFL API docs](https://api-portal.tfl.gov.uk/api-details#api=Line&operation=Line_MetaModes)
