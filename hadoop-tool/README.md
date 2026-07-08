# Installation

At hadoop-practice directory, run the following command:

```bash
# Run docker compose
docker-compose up -d --build

# Make directory
docker-compose exec -it namenode hdfs dfs -mkdir -p /steam/raw/reviews

# Upload file data/sample
docker-compose exec -it namenode sh -c "hdfs dfs -put /opt/data/sample/* /steam/raw/reviews/"

# Show files
docker-compose exec -it namenode hdfs dfs -ls /steam/raw/reviews


# Upload file metadata
## Make metadata folder
docker-compose exec -it namenode hdfs dfs -mkdir /steam/metadata

## Push games.json
docker-compose exec -it namenode hdfs dfs -put /opt/data/games.json /steam/metadata

# Push all file merged

```

# Demo

1. A datanode is dead

```bash
# Make a datanode dead
docker-compose stop datanode1

# Check report hdfs
hdfs dfsadmin -report

# Check file
hdfs dfs -cat /steam/raw/reviews/1010290_28.csv
```

# Utils

```bash
# Run bash
docker-compose exec -it namenode /bin/bash

# Xem report hdfs
hdfs dfsadmin -report

hdfs fsck /steam/raw/reviews/test.csv -files -blocks -locations
```
