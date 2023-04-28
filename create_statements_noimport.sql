-- Create tables
-- Droping the tables if they already exists in the db
DROP TABLE IF EXISTS Updates;
DROP TABLE IF EXISTS Activities;
DROP TABLE IF EXISTS Deals;

-- Creating tables

CREATE TABLE Deals (
    DealID INT PRIMARY KEY,
    UserID VARCHAR(60) NOT NULL,
    Status VARCHAR(4) NOT NULL,
    Value FLOAT NOT NULL,
    Currency VARCHAR(3) NOT NULL,
    TotalActivities INT NOT NULL
);

CREATE TABLE Activities (
    ActivityID INT PRIMARY KEY,
    DealID INT REFERENCES Deals(DealID),
    Type VARCHAR NOT NULL,
    MarkedAsDone DATE,
    Deleted BOOLEAN NOT NULL
);

CREATE TABLE Updates (
    DealID INT REFERENCES Deals(DealID),
    Type VARCHAR(20) NOT NULL,
    Old VARCHAR,
    New VARCHAR,
    FOREIGN KEY (DealID) REFERENCES Deals(DealID)
);
