package com.marcreyesph;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcPostgresql {
    private final String DB_URL = "postgres://tpivhqsbtgcmny:92cfbdf76ca77493c3fdbfd3b45c457e89b8fd4c102dc2870d994ec85dc580b9@ec2-54-243-208-234.compute-1.amazonaws.com:5432/d3m9eobk6mkr7h";
    private final String DB_USER = "tpivhqsbtgcmny";
    private String DB_PASSWORD = "92cfbdf76ca77493c3fdbfd3b45c457e89b8fd4c102dc2870d994ec85dc580b9";

    /**
     * Connect to the PostgreSQL database
     *
     * @return a Connection object
     */
    public Connection connect() throws SQLException {
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
    }
}