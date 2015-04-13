Step 1: load in the input data as a Hive table (undocumented)

Step 2: Clean the data
```
-- Clean the input data noting that
--   1) the day and month is of format "01"
--   2) there is no year
CREATE TABLE tim.nightlights_in_clean STORED AS RCFILE
AS SELECT
  CAST(regexp_replace(day, '"', '') AS INT) AS day,
  CAST(regexp_replace(month, '"', '') AS INT) AS month,
  2012 AS year,
  upshapelatitude AS latitude,
  upshapelongitude AS longitude,
  vis
FROM tim.nightlights_in
WHERE
  upshapelatitude IS NOT NULL AND upshapelongitude IS NOT NULL AND
  day IS NOT NULL AND month IS NOT NULL;
```

Step 3: Process the data
```
-- Add the UDTF
ADD JAR hive-udf-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION tiled AS 'com.vizzuality.hadoop.hive.MercatorTilesUDTF';

-- Project the coordinates into mercator, group them by time and pixel and average the
-- visibility
CREATE TABLE tim.india_tiles
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' AS
SELECT
  z,
  tx,
  ty,
  px,
  py,
  day,
  month,
  year,
  AVG(vis) as avg_vis
FROM tim.nightlights_in_clean
  LATERAL VIEW tiled(latitude, longitude, 14) tiled AS z,tx,ty,px,py
GROUP BY
  z, tx, ty, px, py, day, month, year
```
