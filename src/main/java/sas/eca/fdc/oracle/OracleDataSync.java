package sas.eca.fdc.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class OracleDataSync {
    private static final Logger logger = LoggerFactory.getLogger(OracleDataSync.class);
    private static Connection sourceConn;
    private static Connection targetConn;

    public static void main(String[] args) {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("config.properties"));

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

            logger.info("Data synchronization completed successfully.");

        } catch (Exception e) {
            logger.error(" Error: " + e.getMessage(), e);
        } finally {
            closeConnection(sourceConn);
            closeConnection(targetConn);
        }
    }

    private static void syncData(String selectQuery, String mergeQuery, boolean batchMode, int batchSize) {
        int id = 0;
        String name = null;
        try (Statement stmt = sourceConn.createStatement();
                ResultSet rs = stmt.executeQuery(selectQuery);
                PreparedStatement ps = targetConn.prepareStatement(mergeQuery)) {

            int count = 0;

            while (rs.next()) {
                id = rs.getInt("ID");
                name = rs.getString("NAME");
                ps.setInt(1, id);
                ps.setString(2, name);

                String queryInfo = String.format("Executing Query: %s | Values: [ID=%d, NAME=%s]", mergeQuery, id,
                        name);
                logger.info(queryInfo);

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
            String failedQueryInfo = String.format("Failed Query: %s | Values: [ID=%d, NAME=%s]", mergeQuery, id, name);
            logger.error("SQL Error while syncing data: " + e.getMessage(), e);
            logger.error(failedQueryInfo);
        }
    }

    private static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.warn("Error closing connection: " + e.getMessage(), e);
            }
        }
    }
}
