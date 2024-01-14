-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS "market_data" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

-- Create Tick table
CREATE TABLE IF NOT EXISTS market_data.TB_TICK(
        SYMBOL ascii,
        TIMESTAMP timestamp,
        PRICE decimal,
        QUANTITY decimal,
        PRIMARY KEY((SYMBOL), TIMESTAMP)
    )
WITH CLUSTERING ORDER BY(TIMESTAMP ASC);

-- Create M1 table
CREATE TABLE IF NOT EXISTS market_data.TB_M1(
        SYMBOL ascii,
        TIMESTAMP timestamp,
        OPEN decimal,
        HIGH decimal,
        LOW decimal,
        CLOSE decimal,
        volume decimal,
        PRIMARY KEY((SYMBOL), TIMESTAMP)
    )
WITH CLUSTERING ORDER BY(TIMESTAMP ASC);

-- Create M5 table
CREATE TABLE IF NOT EXISTS market_data.TB_M5(
        SYMBOL ascii,
        TIMESTAMP timestamp,
        OPEN decimal,
        HIGH decimal,
        LOW decimal,
        CLOSE decimal,
        volume decimal,
        PRIMARY KEY((SYMBOL), TIMESTAMP)
    )
WITH CLUSTERING ORDER BY(TIMESTAMP ASC);

-- Create M15 table
CREATE TABLE IF NOT EXISTS market_data.TB_M15(
        SYMBOL ascii,
        TIMESTAMP timestamp,
        OPEN decimal,
        HIGH decimal,
        LOW decimal,
        CLOSE decimal,
        volume decimal,
        PRIMARY KEY((SYMBOL), TIMESTAMP)
    )
WITH CLUSTERING ORDER BY(TIMESTAMP ASC);

-- Create M30 table
CREATE TABLE IF NOT EXISTS market_data.TB_M30(
        SYMBOL ascii,
        TIMESTAMP timestamp,
        OPEN decimal,
        HIGH decimal,
        LOW decimal,
        CLOSE decimal,
        volume decimal,
        PRIMARY KEY((SYMBOL), TIMESTAMP)
    )
WITH CLUSTERING ORDER BY(TIMESTAMP ASC);

-- Create H1 table
CREATE TABLE IF NOT EXISTS market_data.TB_H1(
        SYMBOL ascii,
        TIMESTAMP timestamp,
        OPEN decimal,
        HIGH decimal,
        LOW decimal,
        CLOSE decimal,
        volume decimal,
        PRIMARY KEY((SYMBOL), TIMESTAMP)
    )
WITH CLUSTERING ORDER BY(TIMESTAMP ASC);

-- Create H4 table
CREATE TABLE IF NOT EXISTS market_data.TB_H4(
        SYMBOL ascii,
        TIMESTAMP timestamp,
        OPEN decimal,
        HIGH decimal,
        LOW decimal,
        CLOSE decimal,
        volume decimal,
        PRIMARY KEY((SYMBOL), TIMESTAMP)
    )
WITH CLUSTERING ORDER BY(TIMESTAMP ASC);

-- Create D table
CREATE TABLE IF NOT EXISTS market_data.TB_D(
        SYMBOL ascii,
        TIMESTAMP timestamp,
        OPEN decimal,
        HIGH decimal,
        LOW decimal,
        CLOSE decimal,
        volume decimal,
        PRIMARY KEY((SYMBOL), TIMESTAMP)
    )
WITH CLUSTERING ORDER BY(TIMESTAMP ASC);