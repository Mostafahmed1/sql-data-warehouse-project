/*
Create datawarehouse from scratch with its there layers (Bronze,Silver,Gold)
There is a part of code can be added for check if the database already exist or not
,but also you can check manually
*/

Use master;

-- create the datawarehouse 
Create database DataWarehouse;
Use DataWarehouse;

--creating the schemas
create Schema Bronze;
Go
create Schema Silver,
Go
create Schema Gold;
