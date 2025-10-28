package sas.eca.fdc.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseManager {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseManager.class);

    public static Connection getConnection(Properties props, String type) throws SQLException {
        String prefix = type.toLowerCase() + ".";
        return DriverManager.getConnection(
                props.getProperty(prefix + "url"),
                props.getProperty(prefix + "user"),
                props.getProperty(prefix + "password"));
    }

    public static void rollback(Connection conn) {
        if (conn != null) {
            try {
                logger.info("Rolling back transaction.");
                conn.rollback();
            } catch (SQLException e) {
                logger.error("Error during transaction rollback: " + e.getMessage(), e);
            }
        }
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.warn("Error closing connection: " + e.getMessage(), e);
            }
        }
    }
}