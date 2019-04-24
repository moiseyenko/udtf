CREATE INDEX idx_uatxf_cityid
ON TABLE uatxf (cityid)
AS 'COMPACT'
WITH DEFERRED REBUILD;

ALTER INDEX idx_uatxf_cityid ON uatxf REBUILD;
