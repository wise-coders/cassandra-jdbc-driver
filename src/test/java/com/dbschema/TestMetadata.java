package com.dbschema;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;

/**
 * This test works with a local docker container.
 *
 * Copyright Wise Coders GmbH. The Cassandra JDBC driver is build to be used with DbSchema Database Designer https://dbschema.com
 * Free to use by everyone, code modifications allowed only to
 * the public repository https://github.com/wise-coders/cassandra-jdbc-driver
 */

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
        stmt.executeQuery("CREATE KEYSPACE IF NOT EXISTS dbschema WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 3 } AND DURABLE_WRITES = false");
        stmt.executeQuery( "CREATE TABLE IF NOT EXISTS dbschema.cyclist_category ( category text, points int, id UUID, lastname text, PRIMARY KEY (category, points)) WITH CLUSTERING ORDER BY (points DESC)");
        stmt.executeQuery( "CREATE TABLE IF NOT EXISTS dbschema.rank_by_year_and_name ( race_year int, race_name text,  cyclist_name text, rank int,  PRIMARY KEY ((race_year, race_name), rank) )");
        stmt.executeQuery("CREATE TYPE IF NOT EXISTS dbschema.basic_info ( birthday timestamp,  nationality text, weight text, height text)");
        stmt.executeQuery("CREATE TABLE IF NOT EXISTS dbschema.cyclist_stats ( id uuid PRIMARY KEY, lastname text, basics FROZEN<basic_info>) ");
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
        printResultSet( stmt.executeQuery("DESC dbschema.cyclist_category") );
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
