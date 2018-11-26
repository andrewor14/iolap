set hive.map.aggr=false;
set hive.groupby.skewindata=true;

CREATE TABLE DEST1(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST2(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM SRC
INSERT OVERWRITE TABLE DEST1 SELECT SRC.key, COUNT(DISTINCT SUBSTR(SRC.value,5)) GROUP BY SRC.key
INSERT OVERWRITE TABLE DEST2 SELECT SRC.key, COUNT(DISTINCT SUBSTR(SRC.value,5)) GROUP BY SRC.key;

FROM SRC
INSERT OVERWRITE TABLE DEST1 SELECT SRC.key, COUNT(DISTINCT SUBSTR(SRC.value,5)) GROUP BY SRC.key
INSERT OVERWRITE TABLE DEST2 SELECT SRC.key, COUNT(DISTINCT SUBSTR(SRC.value,5)) GROUP BY SRC.key;

SELECT DEST1.* FROM DEST1;
SELECT DEST2.* FROM DEST2;

set hive.multigroupby.singlereducer=false;

EXPLAIN
FROM SRC
INSERT OVERWRITE TABLE DEST1 SELECT SRC.key, COUNT(DISTINCT SUBSTR(SRC.value,5)) GROUP BY SRC.key
INSERT OVERWRITE TABLE DEST2 SELECT SRC.key, COUNT(DISTINCT SUBSTR(SRC.value,5)) GROUP BY SRC.key;

FROM SRC
INSERT OVERWRITE TABLE DEST1 SELECT SRC.key, COUNT(DISTINCT SUBSTR(SRC.value,5)) GROUP BY SRC.key
INSERT OVERWRITE TABLE DEST2 SELECT SRC.key, COUNT(DISTINCT SUBSTR(SRC.value,5)) GROUP BY SRC.key;

SELECT DEST1.* FROM DEST1;
SELECT DEST2.* FROM DEST2;
