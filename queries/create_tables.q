create external table s1 (
BidID STRING,
date_time STRING,
iPinYouID STRING,
User_Agent STRING,
IP STRING,
RegionID SMALLINT,
CityID SMALLINT,
AdExchange TINYINT,
Domain STRING,
AD_URL STRING,
Anonymous_URL STRING,
Ad_Slot_ID STRING,
Ad_Slot_Width INT,
Ad_Slot_Height INT,
Ad_Slot_Visibility STRING,
Ad_Slot_Format STRING,
Ad_Slot_Floor_Price DOUBLE,
Bidding_Price DOUBLE,
Paying_Price DOUBLE,
Landing_Page_URL STRING,
User_Profile_IDs BIGINT)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
LOCATION '/common/fulldataset/s1/';

create external table s2 (
BidID STRING,
date_time STRING,
iPinYouID STRING,
User_Agent STRING,
IP STRING,
RegionID SMALLINT,
CityID SMALLINT,
AdExchange TINYINT,
Domain STRING,
AD_URL STRING,
Anonymous_URL STRING,
Ad_Slot_ID STRING,
Ad_Slot_Width INT,
Ad_Slot_Height INT,
Ad_Slot_Format INT,
Ad_Slot_Floor_Price DOUBLE,
Bidding_Price DOUBLE,
Landing_Page_URL STRING,
Paying_Price DOUBLE,
Advertiser_ID STRING,
User_Profile_IDs BIGINT)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
LOCATION '/common/fulldataset/s2/';

create external table s3 (
BidID STRING,
date_time STRING,
iPinYouID STRING,
User_Agent STRING,
IP STRING,
RegionID SMALLINT,
CityID SMALLINT,
AdExchange TINYINT,
Domain STRING,
AD_URL STRING,
Anonymous_URL STRING,
Ad_Slot_ID STRING,
Ad_Slot_Width INT,
Ad_Slot_Height INT,
Ad_Slot_Visibility STRING,
Ad_Slot_Format STRING,
Ad_Slot_Floor_Price DOUBLE,
Bidding_Price DOUBLE,
Paying_Price DOUBLE,
Advertiser_ID STRING,
User_Profile_IDs BIGINT)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
LOCATION '/common/fulldataset/s3/';

create external table citysrc(
Id SMALLINT,
Name STRING)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
LOCATION '/common/fulldataset/citysrc/';

create table city(
Id SMALLINT,
Name STRING)
CLUSTERED BY (Id) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ("transactional"="true");

CREATE TABLE uatxf(
User_Agent STRING,
CityID SMALLINT)
PARTITIONED BY (season SMALLINT)
CLUSTERED BY (CityID) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ("transactional"="true");

CREATE TABLE ua_raw(
User_Agent STRING,
CityID SMALLINT,
Season SMALLINT);

CREATE TABLE ua_partition(
User_Agent STRING,
CityID SMALLINT)
PARTITIONED BY (Season SMALLINT);

CREATE TABLE ua_bucketed(
User_Agent STRING,
CityID SMALLINT,
Season SMALLINT)
CLUSTERED BY (CityID) INTO 4 BUCKETS;

CREATE TABLE ua_partition_bucketed(
User_Agent STRING,
CityID SMALLINT)
PARTITIONED BY (Season SMALLINT)
CLUSTERED BY (CityID) INTO 4 BUCKETS;
