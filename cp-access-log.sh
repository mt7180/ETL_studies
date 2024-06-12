cut -d"#" -f1-4 web-server-access-log.txt > extracted-data.txt
tr "#" "," < extracted-data.txt > transformed-data.csv
echo "\c template1;\COPY access_log  FROM '/home/project/transformed-data.csv' DELIMITERS ',' CSV HEADER;" | psql --username=postgres --host=localhost