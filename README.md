# QuestDB Dataset utilities

Some misc scripts for working with demo QuestDB datasets. This is a personal project for testing out things.

It is messy and not production ready.

By all means you are welcome to use it, but code is not optimal and it lacks any explanation, so be very careful out there.

## qdb_table_downloaded

Downloads the passed query, paginating every 1000000 million rows, and outputs a csv

## tfl_dataset

Downloads data from [TFL's OpenApi](https://api-portal.tfl.gov.uk/api-details#api=Mode&operation=Mode_Arrivals) and stores it on QuestDB. You can pass the transport "mode", for example _tube_ or _bus_ as seen at the [TFL API docs](https://api-portal.tfl.gov.uk/api-details#api=Line&operation=Line_MetaModes)

## License
Apache 2 License
