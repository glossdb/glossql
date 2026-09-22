-- Runs once, when the Postgres volume is first created. One server,
-- two databases: the catalog the live tests open with a local
-- warehouse, and a second for the Azurite warehouse (a catalog
-- remembers its warehouse — the two never share one).
CREATE DATABASE glossql_az;
