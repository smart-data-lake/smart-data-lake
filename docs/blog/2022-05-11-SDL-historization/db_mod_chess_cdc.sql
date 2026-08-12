USE foobar
GO
UPDATE dbo.chess_cdc SET victory_status = "overTime" Where victory_status = "outoftime"
DELETE FROM dbo.chess_cdc WHERE id = "009mKOEz"
GO
