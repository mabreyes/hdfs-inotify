DROP TABLE IF EXISTS file_activity;

CREATE TABLE IF NOT EXISTS file_activity (
    fileChangeId SERIAL PRIMARY KEY,
    transactionId INTEGER NOT NULL,
    eventType VARCHAR(100) NOT NULL,
    path VARCHAR(1000) NOT NULL,
    ownerName VARCHAR(500) NULL,
    cTime TIMESTAMP NULL,
    timeStamp TIMESTAMP NULL,
    fileSize INTEGER NULL,
    dstPath VARCHAR(1000) NULL,
    srcPath VARCHAR(1000) NULL
);