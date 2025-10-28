package sas.eca.fdc.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sas.eca.fdc.db.DatabaseManager;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class OracleDataSync {
    private static final Logger logger = LoggerFactory.getLogger(OracleDataSync.class);

    public static void main(String[] args) {
        try {
            Properties props = initialize();
            runPreSyncTasks(props);
            syncData(props);
            logger.info("Data synchronization completed successfully.");
        } catch (Exception e) {
            logger.error("Error during data synchronization process: " + e.getMessage(), e);
        }
    }

    private static void runPreSyncTasks(Properties props) {
        if (Boolean.parseBoolean(props.getProperty("truncate.mode", "false"))) {
            String tableName = props.getProperty("truncate.table");
            if (tableName != null && !tableName.trim().isEmpty()) {
                backupTable(props, tableName);
            }
            truncateTable(props);
        }
    }

    private static Properties initialize() throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream("config.properties"));

        // Set system properties for logback
        System.setProperty("log.policy", props.getProperty("log.policy", "date"));
        System.setProperty("log.maxFileSize", props.getProperty("log.maxFileSize", "100MB"));

        return props;
    }

    private static void truncateTable(Properties props) {
        String truncateQuery = props.getProperty("truncate.query");
        if (truncateQuery == null || truncateQuery.trim().isEmpty()) {
            logger.warn("Truncate query is empty. Skipping truncate operation.");
            return;
        }

        Connection conn = null;
        try {
            conn = DatabaseManager.getConnection(props, "target");
            try (Statement stmt = conn.createStatement()) {
                logger.info("Executing Query: {}", truncateQuery);
                stmt.execute(truncateQuery);
                logger.info("Table truncated successfully.");
            }
        } catch (SQLException e) {
            logger.error("SQL Error while truncating table: " + e.getMessage(), e);
            logger.error("Failed Query: {}", truncateQuery);
        } catch (Exception e) {
            logger.error("An unexpected error occurred during truncate: " + e.getMessage(), e);
        } finally {
            DatabaseManager.closeConnection(conn);
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

                    String queryInfo = mergeQuery.replaceFirst("\\?", String.valueOf(id)).replaceFirst("\\?",
                            "'" + name + "'");
                    logger.info("Executing Query: {}", queryInfo);

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
            String failedQueryInfo = mergeQuery.replaceFirst("\\?", String.valueOf(id)).replaceFirst("\\?",
                    "'" + name + "'");
            logger.error("SQL Error while syncing data: " + e.getMessage(), e);
            logger.error("Failed Query: {}", failedQueryInfo);
            DatabaseManager.rollback(targetConn);
        } catch (Exception e) {
            logger.error("An unexpected error occurred during data sync: " + e.getMessage(), e);
            DatabaseManager.rollback(targetConn);
        } finally {
            DatabaseManager.closeConnection(sourceConn);
            DatabaseManager.closeConnection(targetConn);
        }
    }

    private static void backupTable(Properties props, String tableName) {
        Connection targetConn = null;
        try {
            targetConn = DatabaseManager.getConnection(props, "target");
            logger.info("Backup started for table: {}", tableName);

            Statement statement = targetConn.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM " + tableName);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                StringBuilder insertStatement = new StringBuilder("INSERT INTO " + tableName + " VALUES (");

                for (int i = 1; i <= columnCount; i++) {
                    Object value = resultSet.getObject(i);
                    if (value == null) {
                        insertStatement.append("NULL");
                    } else if (value instanceof String || value instanceof java.util.Date
                            || value instanceof java.sql.Timestamp) {
                        insertStatement.append("'").append(value.toString().replace("'", "''")).append("'");

                    } else {
                        insertStatement.append(value);
                    }
                    if (i < columnCount) {
                        insertStatement.append(", ");
                    }
                }
                insertStatement.append(");");

                System.out.println(insertStatement.toString());
            }
            resultSet.close();
            statement.close();

        } catch (SQLException e) {
            logger.error("SQL Error during backup: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("An unexpected error occurred during backup: " + e.getMessage(), e);
        } finally {
            DatabaseManager.closeConnection(targetConn);
        }
    }
}
