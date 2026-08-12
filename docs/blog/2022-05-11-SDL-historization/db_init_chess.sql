-- initialization script for MS SQL server
--   creating a table imported from a downloaded CSV
--   which originates form (https://www.kaggle.com/datasets/datasnaek/chess)
CREATE DATABASE foobar
GO
USE foobar
GO
CREATE TABLE dbo.chess (
    id varchar(8) NULL,
    rated varchar(5) Null,
    created_at real Null,
    last_move_at real Null,
    turns int Null,
    victory_status varchar(25),
    winner varchar(5) Null,
    increment_code varchar(7) Null,
    white_id varchar(25) Null,
    white_rating int NULL,
    black_id varchar(25) NULL,
    black_rating int NULL,
    moves varchar(1415) NULL,
    opening_eco varchar(3) Null,
    opening_name varchar(256) Null,
    opening_ply int Null
)
GO
BULK INSERT dbo.chess
    FROM '/data/chess.csv'
    WITH
    (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',  --CSV field delimiter
    ROWTERMINATOR = '\n',   --Use to shift the control to next row
    ERRORFILE = '/tmp/chessErrorRows.csv',
    TABLOCK
    )
GO

-- here we use the MS SQL server feature CDC (change data capture)
--   it needs to be enabled and the data need to be deduplicated first
--   further the SQL agent need to be enabled,
--     e.g. by calling `/opt/mssql/bin/mssql-conf set sqlagent.enabled true` on the CLI
-- for the deduplication the above tabe is utilized.
EXEC sys.sp_cdc_enable_db
GO
ALTER DATABASE foobar
SET ALLOW_SNAPSHOT_ISOLATION ON
GO
ALTER DATABASE foobar
SET READ_COMMITTED_SNAPSHOT ON
GO

CREATE TABLE dbo.chess_cdc (
    id varchar(8) PRIMARY KEY,
    rated varchar(5) Null,
    created_at real Null,
    last_move_at real Null,
    turns int Null,
    victory_status varchar(25),
    winner varchar(5) Null,
    increment_code varchar(7) Null,
    white_id varchar(25) Null,
    white_rating int NULL,
    black_id varchar(25) NULL,
    black_rating int NULL,
    moves varchar(1415) NULL,
    opening_eco varchar(3) Null,
    opening_name varchar(256) Null,
    opening_ply int Null
)
GO

EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name   = N'chess_cdc',
@role_name     = N'null',
@supports_net_changes = 1
GO
GRANT SELECT ON SCHEMA :: [cdc] TO abuser;
GO
INSERT INTO dbo.chess_cdc
SELECT id,
        MIN(rated) AS rated,
        MIN(created_at) AS created_at,
        MIN(last_move_at) AS last_move_at,
        MIN(turns) AS turns,
        MIN(victory_status) AS victory_status,
        MIN(winner) AS winner,
        MIN(increment_code) AS increment_code,
        MIN(white_id) AS white_id,
        MIN(white_rating) AS white_rating,
        MIN(black_id) AS black_id,
        MIN(black_rating) AS black_rating,
        MIN(moves) AS moves,
        MIN(opening_eco) AS opening_eco,
        MIN(opening_name) AS opening_name,
        MIN(opening_ply) AS opening_ply
    FROM chess
    GROUP BY id
