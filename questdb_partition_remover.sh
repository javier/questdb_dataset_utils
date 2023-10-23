#!/bin/bash

# This script needs both curl and jq installed.
# It will go over all the tables with daily partitioning and will remove all partitions older than 5000 days (change that after you are sure it works as you expect)
# It uses jq to parse the JSON output from the REST API, extracting the "dataset" element and flatten all the rows.
# Then it reads line by line and calls the QuestDB API with each ALTER TABLE statement.


# We get all the tables with daily partitioning and compose the ALTER TABLE statements

TABLES=`curl -G  --data-urlencode "query=with daily_tables AS (
select name, designatedTimestamp, timestamp_floor('d',dateadd('d', -5000, systimestamp())) as deadline from tables where partitionBy = 'DAY'
)
select CONCAT('ALTER TABLE ', name, ' DROP PARTITION WHERE ', designatedTimestamp, ' <= ', deadline) FROM daily_tables;" "http://localhost:9000/exec?nm=true"|jq ".dataset | flatten[]"`


# Splitting the output line by line and issuing the ALTER TABLE
printf '%s\n' "$TABLES" |
while IFS= read -r sql; do
        # echo $sql #uncomment if you want to output each statement we are sending
        #we need to remove starting and trailing double quote from the line, so using :1:-1 syntax
        curl -G --data-urlencode "query=${sql:1:-1}" http://localhost:9000/exec
done

