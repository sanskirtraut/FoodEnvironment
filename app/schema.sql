PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS Competes_With;
DROP TABLE IF EXISTS Food_Establishment;
DROP TABLE IF EXISTS Obesity_Statistic;
DROP TABLE IF EXISTS Category_Type;
DROP TABLE IF EXISTS Census_Tract;

CREATE TABLE Census_Tract (
    tract_id      TEXT PRIMARY KEY,
    population    INTEGER,
    median_income INTEGER,
    county        TEXT,
    state         TEXT,
    latitude      REAL,
    longitude     REAL
);

CREATE TABLE Category_Type (
    type_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    category_name TEXT UNIQUE NOT NULL,
    health_score  INTEGER NOT NULL
);

CREATE TABLE Obesity_Statistic (
    stat_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    obesity_rate  REAL,
    year_recorded INTEGER,
    tract_id      TEXT NOT NULL,
    FOREIGN KEY (tract_id) REFERENCES Census_Tract(tract_id)
);

CREATE TABLE Food_Establishment (
    store_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL,
    latitude   REAL,
    longitude  REAL,
    address    TEXT,
    zipcode    TEXT,
    fresh_food INTEGER DEFAULT 0,
    tract_id   TEXT,
    type_id    INTEGER NOT NULL,
    FOREIGN KEY (tract_id) REFERENCES Census_Tract(tract_id),
    FOREIGN KEY (type_id) REFERENCES Category_Type(type_id)
);

CREATE TABLE Competes_With (
    store_1_id INTEGER NOT NULL,
    store_2_id INTEGER NOT NULL,
    PRIMARY KEY (store_1_id, store_2_id),
    FOREIGN KEY (store_1_id) REFERENCES Food_Establishment(store_id),
    FOREIGN KEY (store_2_id) REFERENCES Food_Establishment(store_id)
);

CREATE INDEX idx_est_tract ON Food_Establishment(tract_id);
CREATE INDEX idx_est_zip ON Food_Establishment(zipcode);
CREATE INDEX idx_est_type ON Food_Establishment(type_id);
CREATE INDEX idx_obesity_tract ON Obesity_Statistic(tract_id);
