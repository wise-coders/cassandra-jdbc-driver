package com.dbschema;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;


/**
 * This test are working with a local docker container.
 **/
public class TestMetadata {

    private Connection con;

    private static final String urlWithoutAuth = "jdbc:cassandra://localhost/system?expand=true";

    // THIS TESTS ARE USING A LOCAL INSTALLED CASSANDRA (I USE DOCKER)
    @Before
    public void setUp() throws ClassNotFoundException, SQLException {
        Class.forName("com.dbschema.CassandraJdbcDriver");
        con = DriverManager.getConnection( urlWithoutAuth, "cassandra", "cassandra");
        Statement stmt = con.createStatement();
        stmt.execute("SELECT cql_version FROM system.local");
        stmt.close();
    }


    @Test
    public void testMetaData() throws Exception {
        printResultSet( con.getMetaData().getCatalogs() );
        printResultSet( con.getMetaData().getTables("dbschema", null, null, null ));
    }

    @Test
    public void testReadTimestamp() throws Exception {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT name, videoID, added_date FROM killrvideo.user_videos");
        printResultSet( rs );
        stmt.close();
    }

    @Test
    public void testFind() throws Exception {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT cql_version FROM system.local");
        printResultSet( rs );
        stmt.close();
    }

    @Test
    public void testDescribe() throws Exception {
        Statement stmt = con.createStatement();
        printResultSet( stmt.executeQuery("DESC dbschema") );
        stmt.close();
    }

    private static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++ ){
            System.out.printf( "%30s", metaData.getColumnName( i)+ "(" + metaData.getColumnType(i) + ")");
        }
        System.out.println();
        while ( rs.next() ){
            for (int i = 1; i <= metaData.getColumnCount(); i++ ) {
                System.out.printf("%30s", rs.getString(i));
            }
            System.out.println();
        }
    }
}
