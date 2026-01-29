-- CREATE EXTERNAL TABLE
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-485523.zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://zoomcamp-serogonpi-demo/yellow_tripdata_2024-*.parquet', 'gs://zoomcamp-serogonpi-demo/yellow_tripdata_2024-*.csv']
);

-- CREATE REGULAR TABLE
CREATE OR REPLACE TABLE zoomcamp.yellow_tripdata AS
  SELECT * 
  FROM zoomcamp.external_yellow_tripdata;

-- COUNT OF RECORDS: 20332093
SELECT COUNT(*) 
FROM zoomcamp.external_yellow_tripdata;

-- COUNT DISTINCT NUMBER OF PULocationsIDs IN EXTERNAL TABLE: 0 B
SELECT DISTINCT COUNT(PULocationID)
FROM zoomcamp.external_yellow_tripdata;

-- COUNT DISTINCT NUMBER OF PULocationsIDs IN REGULAR TABLE: 152.12 MB
SELECT DISTINCT COUNT(PULocationID)
FROM zoomcamp.yellow_tripdata;

-- RETRIEVE PULocationID 
SELECT PULocationID
FROM zoomcamp.yellow_tripdata;

-- RETRIEVE PULocationID and DOLocationID
SELECT PULocationID, DOLocationID
FROM zoomcamp.yellow_tripdata;

-- HOW MANY RECORDS HAVE A FARE_AMOUNT OF 0?: 8333
SELECT COUNT(*)
FROM zoomcamp.yellow_tripdata
WHERE fare_amount = 0;

-- CREATE OPTIMIZED TABLE PARTITIONED BY tpep_dropoff_datetime AND CLUSTERED BY VendorID
CREATE OR REPLACE TABLE zoomcamp.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM zoomcamp.yellow_tripdata;

-- query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
-- 310.24 MB
SELECT DISTINCT COUNT(VendorID)
FROM zoomcamp.yellow_tripdata
WHERE CAST(tpep_dropoff_datetime AS DATE) BETWEEN '2024-03-01' AND '2024-03-15';

-- 26.84 MB
SELECT DISTINCT COUNT(VendorID)
FROM zoomcamp.yellow_tripdata_partitioned_clustered
WHERE CAST(tpep_dropoff_datetime AS DATE) BETWEEN '2024-03-01' AND '2024-03-15';

-- BONUS QUESTION: 0 B
SELECT COUNT(*) 
FROM zoomcamp.yellow_tripdata;