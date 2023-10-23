# This script needs both curl and jq installed.
# It will go over all the tables with daily partitioning and will remove all partitions older than 5000 days (change that after you are sure it works as you expect)
# It sends the initial statement to QuestDB’s API and uses jq to parse the JSON output from the REST API.
# Then it extracts the "dataset" element and flatten all the rows.
# Then it reads line by line and calls the QuestDB API with each ALTER TABLE statement.
# Since the output comes quoted with double quotes, before it passes it to the API it trims out the first and last characters (the quotes), using ${sql:1:-1}

 curl -G  --data-urlencode "query=with daily_tables AS (                                                                                                                              🙈
select name, designatedTimestamp, timestamp_floor('d',dateadd('d', -5000, systimestamp())) as deadline from tables where partitionBy = 'DAY'
)
select CONCAT('ALTER TABLE ', name, ' DROP PARTITION WHERE ', designatedTimestamp, ' <= ', deadline) FROM daily_tables;" "http://localhost:9000/exec?nm=true"|jq ".dataset | flatten[]" | {while read sql;do for item in $sql
do
        curl -G --data-urlencode "query=${sql:1:-1}" http://localhost:9000/exec
done;done}
