package sas.eca.fdc.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sas.eca.fdc.db.DatabaseManager;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class OracleDataSync {
    private static final Logger logger = LoggerFactory.getLogger(OracleDataSync.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("config.properties"));

            // Set system properties for logback
            System.setProperty("log.policy", props.getProperty("log.policy", "date"));
            System.setProperty("log.maxFileSize", props.getProperty("log.maxFileSize", "100MB"));

            syncData(props);

            logger.info("Data synchronization completed successfully.");

        } catch (Exception e) {
            logger.error(" Error: " + e.getMessage(), e);
        }
    }

    private static void syncData(Properties props) {
        String selectQuery = props.getProperty("select.query");
        String mergeQuery = props.getProperty("merge.query");
        boolean batchMode = Boolean.parseBoolean(props.getProperty("batch.mode", "false"));
        int batchSize = Integer.parseInt(props.getProperty("batch.size", "100"));

        int id = 0;
        String name = null;
        Connection sourceConn = null;
        Connection targetConn = null;

        try {
            sourceConn = DatabaseManager.getConnection(props, "source");
            targetConn = DatabaseManager.getConnection(props, "target");
            targetConn.setAutoCommit(false);

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

            }
        } catch (SQLException e) {
            String failedQueryInfo = String.format("Failed Query: %s | Values: [ID=%d, NAME=%s]", mergeQuery, id, name);
            logger.error("SQL Error while syncing data: " + e.getMessage(), e);
            logger.error(failedQueryInfo);
        } finally {
            DatabaseManager.closeConnection(sourceConn);
            DatabaseManager.closeConnection(targetConn);
        }
    }
}
