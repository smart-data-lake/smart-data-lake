USE foobar
GO
INSERT dbo.drinks (id, item, category, price) VALUES (5, "virgin mojito", "non-alcohol", "9")
GO
UPDATE dbo.drinks SET price = "0" Where item = "water"
GO
DELETE FROM dbo.drinks WHERE id = 4
GO
