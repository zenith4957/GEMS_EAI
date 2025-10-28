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
        String trucateQuery = props.getProperty("truncate.query");
        boolean batchMode = Boolean.parseBoolean(props.getProperty("batch.mode", "false"));
        int batchSize = Integer.parseInt(props.getProperty("batch.size", "100"));

        int id = 0;
        String name = null;
        Connection sourceConn = null;
        Connection targetConn = null;
        if (truncateMode){
            logger.info("backup start");
            backupTable("TARGET_TABLE");
            logger.info("backup end")

            logger.info("Start truncate table : " + trucateQuery)
            try(Statement truncStmt = targetConn.createStatement()){
                truncStmt.executeUpdate(truncateQuery);
                targetConn.commit();
                logger.inf("Completed truncate table");

            } catch (SQLException e){
                logger.error("SQL Error while truncate table: " + e.getMessage(), e);
                System.out.println("Truncate fils.Process killed");
                System.exit(-1);
            }
        }

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
        } finally {
            DatabaseManager.closeConnection(sourceConn);
            DatabaseManager.closeConnection(targetConn);
        }
    }

    private static void backupTable(String tableName) {

        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            String formattedString = sdf.format(new Timestamp(System.currentTimeMillis()));
            logger.info(formattedString);

            targetConn = DatabaseManager.getConnection(props, "target");

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
            e.printStackTrace();
        }
    }
}
