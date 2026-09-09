-- Runs once, when the Postgres volume is first created. One server,
-- three databases: the catalog the live tests open with a local
-- warehouse, a second for the Azurite warehouse (a catalog remembers
-- its warehouse — the two never share one), and Lakekeeper's own.
CREATE DATABASE glossql_az;
CREATE ROLE lakekeeper LOGIN PASSWORD 'lakekeeper';
CREATE DATABASE lakekeeper OWNER lakekeeper;
