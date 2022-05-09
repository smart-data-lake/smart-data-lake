CREATE DATABASE foobar
GO
USE foobar
go
CREATE TABLE dbo.drinks (id int Null, item varchar(25) PRIMARY KEY NOT NULL, category varchar(25) NOT NULL, price int NULL)
GO
INSERT dbo.drinks (id, item, category, price) VALUES (1, "water", "refresh", "2")
INSERT dbo.drinks (id, item, category, price) VALUES (2, "homemade ice tea", "refresh", "5")
INSERT dbo.drinks (id, item, category, price) VALUES (3, "coffee", "hot beverage", "3")
INSERT dbo.drinks (id, item, category, price) VALUES (4, "house wine", "alcoholic", "10")
GO

