package com.dbschema;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class TestMetadata {

    private Connection con;

    private static final String urlWithoutAuth = "jdbc:cassandra://localhost/system?expand=true";

    @Before
    public void setUp() throws ClassNotFoundException, SQLException {
        Class.forName("com.dbschema.CassandraJdbcDriver");
        con = DriverManager.getConnection( urlWithoutAuth, null, null);
        Statement stmt = con.createStatement();
        stmt.execute("SELECT cql_version FROM system.local");
        stmt.close();
    }

    @Test
    public void testFind() throws Exception {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT cql_version FROM system.local");
        printResultSet( rs );
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
