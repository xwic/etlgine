package de.xwic.etlgine.demo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 */
public class DemoDatabaseUtil {
    private static final Log log = LogFactory.getLog(DemoDatabaseUtil.class);

    public static void prepareDB(String conDriver, String conUrl) {
        Connection c = null;
        Statement stmt = null;
        try {
            Class.forName(conDriver);
            c = DriverManager.getConnection(conUrl);
            log.info("Opened DEMO database successfully");

            createDimensionsTable(c);
            createDimensionElementsTable(c);
            createMeasuresTable(c);
            createDimMapTable(c);
            createDimMapElementsTable(c);

            createLoadTestTable(c);
            c.close();

            log.info("DEMO Tables created successfully");

        } catch ( Exception e ) {
            log.error(e);
        }
    }

    private static void createDimensionsTable(Connection con) throws SQLException {
        Statement stmt = null;
        stmt = con.createStatement();
        String sql = "CREATE TABLE IF NOT EXISTS [XCUBE_DIMENSIONS](" +
                "[Key]      [varchar](255)  NOT NULL PRIMARY KEY," +
                "[Title]    [varchar](255)  NULL," +
                "[Sealed]   [bit]           NOT NULL" +
                ")";
        stmt.executeUpdate(sql);
        stmt.close();
    }

    private static void createDimensionElementsTable(Connection con) throws SQLException {
        Statement stmt = null;
        stmt = con.createStatement();
        String sql = "CREATE TABLE IF NOT EXISTS [XCUBE_DIMENSION_ELEMENTS](" +
                "[dbid]             [integer]       NOT NULL PRIMARY KEY AUTOINCREMENT," +
                "[ID]               [varchar](900)  NOT NULL," +
                "[ParentID]         [varchar](900)  NOT NULL," +
                "[DimensionKey]     [varchar](255)  NOT NULL," +
                "[Key]              [varchar](255)  NOT NULL," +
                "[Title]            [varchar](255)  NULL," +
                "[weight]           [float]         NOT NULL," +
                "[order_index]      [int]           NOT NULL DEFAULT ((0)) " +
                ")";
        stmt.executeUpdate(sql);
        stmt.close();
    }

    private static void createMeasuresTable(Connection con) throws SQLException {
        Statement stmt = null;
        stmt = con.createStatement();
        String sql = "CREATE TABLE IF NOT EXISTS [XCUBE_MEASURES] (" +
                "[Key]                  [varchar](255) NOT NULL PRIMARY KEY," +
                "[Title]                [varchar](255) NULL," +
                "[FunctionClass]        [varchar](300) NULL," +
                "[ValueFormatProvider]  [varchar](300) NULL" +
                ")";
        stmt.executeUpdate(sql);
        stmt.close();
    }

    private static void createDimMapTable(Connection con) throws SQLException {
        Statement stmt = null;
        stmt = con.createStatement();
        String sql = "CREATE TABLE IF NOT EXISTS [XCUBE_DIMMAP](" +
                "[DimMapKey]        [varchar](255)  NOT NULL PRIMARY KEY ," +
                "[Description]      [text]          NULL," +
                "[DimensionKey]     [varchar](255)  NOT NULL," +
                "[UnmappedPath]     [varchar](900)  NULL," +
                "[OnUnmapped]       [varchar](50)   NOT NULL CONSTRAINT [DF_XCUBE_DIMMAP_OnUnmapped]  DEFAULT ('CREATE')," +
                "[AutoCreate]       [bit]           NOT NULL" +
                ")";
        stmt.executeUpdate(sql);
        stmt.close();
    }

    private static void createDimMapElementsTable(Connection con) throws SQLException {
        Statement stmt = null;
        stmt = con.createStatement();
        String sql = "CREATE TABLE IF NOT EXISTS [XCUBE_DIMMAP_ELEMENTS](" +
                "[ID]           [integer]       NOT NULL PRIMARY KEY AUTOINCREMENT," +
                "[DimMapKey]    [varchar](255)  NOT NULL," +
                "[Expression]   [text]          NOT NULL," +
                "[isRegExp]     [bit]           NOT NULL," +
                "[IgnoreCase]   [bit]           NOT NULL," +
                "[ElementPath]  [varchar](900)  NULL," +
                "[SkipRecord]   [bit]           NOT NULL," +
                "[order_index]  [int]           NOT NULL DEFAULT ((0))," +
                "[ValidFrom]    [datetime]      NULL," +
                "[ValidTo]      [datetime]      NULL" +
                ")";
        stmt.executeUpdate(sql);
        stmt.close();
    }

    private static void createLoadTestTable(Connection con) throws SQLException {
        Statement stmt = null;
        stmt = con.createStatement();
        String sql =
                "DROP TABLE IF EXISTS [LOAD_TEST_RND];" +
                "CREATE TABLE IF NOT EXISTS [LOAD_TEST_RND](" +
                "[ID]           [integer]       NOT NULL PRIMARY KEY AUTOINCREMENT" +
                ")";
        stmt.executeUpdate(sql);
        stmt.close();
    }

}
