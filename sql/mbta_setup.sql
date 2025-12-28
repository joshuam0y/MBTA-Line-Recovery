CREATE DATABASE IF NOT EXISTS MBTA;
USE DATABASE MBTA;

CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE raw_gse (
    service_date DATE,
    time_period STRING,
    station_name STRING,
    route_or_line STRING,
    gated_entries INTEGER,
    stop_id STRING
);

COPY INTO raw_gse
FROM @%raw_gse
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

CREATE OR REPLACE TABLE clean_gse AS
SELECT
    LOWER(TRIM(station_name)) AS station,
    UPPER(TRIM(route_or_line)) AS line,
    YEAR(service_date) AS year,
    service_date,
    TRY_TO_NUMBER(gated_entries) AS ridership
FROM raw_gse
WHERE gated_entries IS NOT NULL;

CREATE OR REPLACE TABLE line_year_ridership AS
SELECT
    line,
    year,
    SUM(ridership) AS total_ridership
FROM clean_gse
GROUP BY line, year;

CREATE OR REPLACE TABLE line_recovery AS
WITH baseline AS (
    SELECT line, total_ridership AS ridership_2019
    FROM line_year_ridership
    WHERE year = 2019
)
SELECT
    lyr.line,
    lyr.year,
    lyr.total_ridership,
    bl.ridership_2019,
    ROUND(100 * lyr.total_ridership / bl.ridership_2019, 2) AS recovery_pct
FROM line_year_ridership lyr
LEFT JOIN baseline bl
  ON lyr.line = bl.line
ORDER BY lyr.line, lyr.year;

DELETE FROM line_recovery
WHERE line = 'MATTAPAN LINE';

DELETE FROM line_year_ridership
WHERE line = 'MATTAPAN LINE';
