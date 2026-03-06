create database stock_db;

use database stock_db;

create schema stock_db.raw_data;

use database stock_db;
create schema stock_db.analytics;


select * from analytics.stock_prices;


select * from analytics.stock_calcs;

use schema analytics;


show views;

DESCRIBE TABLE stock_calcs;

select * from snapshot.snapshot_stock_calcs;
