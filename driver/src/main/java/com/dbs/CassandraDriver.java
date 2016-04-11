
package com.dbs;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;

import java.net.UnknownHostException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;


public class CassandraDriver implements Driver {
    static {
        try {
            DriverManager.registerDriver( new CassandraDriver());
        } catch ( SQLException ex ){
            ex.printStackTrace();
        }
    }


    /**
     * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
     */
    public Connection connect(String url, Properties info) throws SQLException {
        if ( url != null && acceptsURL( url )){
            try	{
                Cluster cluster = Cluster.builder()
                        .addContactPoint("localhost")
                        .withPort(9042)
                        .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                        .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                        .withCredentials("cassandra", "cassandra")
                        .build();
                Session session = cluster.connect("Dragos");

                return new CassandraConnection(session);
            } catch (UnknownHostException e) {
                throw new SQLException("Unexpected exception: " + e.getMessage(), e);
            }
        }
        return null;
    }


    /**
     * URLs accepted are of the form: jdbc:mongodb://<server>[:27017]/<db-name>
     *
     * @see java.sql.Driver#acceptsURL(java.lang.String)
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith("jdbc:cassandra:");
    }

    /**
     * @see java.sql.Driver#getPropertyInfo(java.lang.String, java.util.Properties)
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException
    {
        return null;
    }

    /**
     * @see java.sql.Driver#getMajorVersion()
     */
    @Override
    public int getMajorVersion()
    {
        return 1;
    }

    /**
     * @see java.sql.Driver#getMinorVersion()
     */
    @Override
    public int getMinorVersion()
    {
        return 0;
    }

    /**
     * @see java.sql.Driver#jdbcCompliant()
     */
    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

}
