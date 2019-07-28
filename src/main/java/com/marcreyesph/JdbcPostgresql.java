package com.marcreyesph;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JdbcPostgresql {

    public static void main(String[] args) {

        String DB_URL = "postgres://tpivhqsbtgcmny:92cfbdf76ca77493c3fdbfd3b45c457e89b8fd4c102dc2870d994ec85dc580b9@ec2-54-243-208-234.compute-1.amazonaws.com:5432/d3m9eobk6mkr7h";
        String DB_USER = "tpivhqsbtgcmny";
        String DB_PASSWORD = "92cfbdf76ca77493c3fdbfd3b45c457e89b8fd4c102dc2870d994ec85dc580b9";

        try (Connection con = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement st = con.createStatement();
             ResultSet rs = st.executeQuery("SELECT VERSION()")) {

            if (rs.next()) {
                System.out.println(rs.getString(1));
            }

        } catch (SQLException ex) {

            Logger lgr = Logger.getLogger(JdbcPostgresql.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }
}