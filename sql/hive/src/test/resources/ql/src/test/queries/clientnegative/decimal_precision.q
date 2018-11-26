DROP TABLE IF EXISTS DECIMAL_PRECISION;

CREATE TABLE DECIMAL_PRECISION(dec decimal) 
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY ' '
STORED AS TEXTFILE;

SELECT dec * 123456789012345678901234567890.123456789bd FROM DECIMAL_PRECISION;

DROP TABLE DECIMAL_PRECISION;
