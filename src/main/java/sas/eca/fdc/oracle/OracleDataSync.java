package sas.eca.fdc.oracle;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class OracleDataSync {
    private static final Logger logger = Logger.getLogger("OracleDataSyncLogger");
    private static Connection sourceConn;
    private static Connection targetConn;

    public static void main(String[] args) {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("config.properties"));
            setupLogger(props.getProperty("log.file", "sync.log"));

            String batchMode = props.getProperty("batch.mode", "false");
            int batchSize = Integer.parseInt(props.getProperty("batch.size", "100"));

            sourceConn = DriverManager.getConnection(
                    props.getProperty("source.url"),
                    props.getProperty("source.user"),
                    props.getProperty("source.password"));

            targetConn = DriverManager.getConnection(
                    props.getProperty("target.url"),
                    props.getProperty("target.user"),
                    props.getProperty("target.password"));

            targetConn.setAutoCommit(false);

            String selectQuery = props.getProperty("select.query");
            String mergeQuery = props.getProperty("merge.query");

            syncData(selectQuery, mergeQuery, Boolean.parseBoolean(batchMode), batchSize);

            logger.info("✅ Data synchronization completed successfully.");

        } catch (Exception e) {
            logger.severe("❌ Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            closeConnection(sourceConn);
            closeConnection(targetConn);
        }
    }

    private static void setupLogger(String logFilePath) throws IOException {
        FileHandler fh = new FileHandler(logFilePath, true);
        fh.setFormatter(new SimpleFormatter());
        logger.addHandler(fh);
    }

    private static void syncData(String selectQuery, String mergeQuery, boolean batchMode, int batchSize) {
        try (Statement stmt = sourceConn.createStatement();
                ResultSet rs = stmt.executeQuery(selectQuery);
                PreparedStatement ps = targetConn.prepareStatement(mergeQuery)) {

            int count = 0;

            while (rs.next()) {
                ps.setInt(1, rs.getInt("ID"));
                ps.setString(2, rs.getString("NAME"));

                if (batchMode) {
                    ps.addBatch();
                    if (++count % batchSize == 0) {
                        ps.executeBatch();
                        targetConn.commit();
                        logger.info("Committed batch of size: " + batchSize);
                    }
                } else {
                    ps.executeUpdate();
                    targetConn.commit();
                }
            }

            if (batchMode && count % batchSize != 0) {
                ps.executeBatch();
                targetConn.commit();
            }

        } catch (SQLException e) {
            logger.severe("SQL Error while syncing data: " + e.getMessage());
            logger.severe("Failed Query: " + mergeQuery);
        }
    }

    private static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.warning("Error closing connection: " + e.getMessage());
            }
        }
    }
}
