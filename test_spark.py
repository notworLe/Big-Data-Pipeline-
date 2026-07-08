import os
from pyspark.sql import SparkSession

def test_hdfs_connection():
    print("Initializing SparkSession...")
    spark = SparkSession.builder \
        .appName("HDFS Connection Test") \
        .master("local[*]") \
        .getOrCreate()
    
    print("SparkSession created successfully.")
    
    # Check if running inside Docker container
    in_docker = os.path.exists('/.dockerenv')
    hdfs_host = "namenode" if in_docker else "localhost"
    
    hdfs_path = f"hdfs://{hdfs_host}:9000/steam/metadata/games_cleaned.json"
    print(f"Reading file from HDFS: {hdfs_path} (in_docker={in_docker})")
    
    try:
        # Read JSON file (JSON Lines format)
        df = spark.read.json(hdfs_path)
        print("Successfully read data from HDFS!")
        print("Schema:")
        df.printSchema()
        print("First 5 rows:")
        df.show(5)
    except Exception as e:
        print("Error connecting to HDFS or reading file:")
        print(e)
    finally:
        spark.stop()

if __name__ == "__main__":
    test_hdfs_connection()
