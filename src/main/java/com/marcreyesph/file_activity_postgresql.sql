DROP TABLE IF EXISTS file_activity_2;

CREATE TABLE IF NOT EXISTS file_activity_2 (
    fileChangeId SERIAL PRIMARY KEY,
    transactionId VARCHAR(1000) NOT NULL,
    eventType VARCHAR(1000) NOT NULL,
    path VARCHAR(1000) NOT NULL,
    ownerName VARCHAR(1000) NULL,
    cTime VARCHAR(1000) NULL,
    timeStamp VARCHAR(1000) NULL,
    fileSize INTEGER NULL,
    dstPath VARCHAR(1000) NULL,
    srcPath VARCHAR(1000) NULL
);