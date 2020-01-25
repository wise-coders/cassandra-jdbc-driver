
package com.dbschema;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.net.UnknownHostException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;


/**
 * Minimal implementation of the JDBC standards for the Cassandra database.
 * This is customized for DbSchema database designer.
 * Connect to the database using a URL like :
 * jdbc:cassandra://host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[keyspace][?options]]
 * The URL excepting the jdbc: prefix is passed as it is to the Cassandra native Java driver.
 */

public class CassandraJdbcDriver implements Driver {
    static {
        try {
            DriverManager.registerDriver( new CassandraJdbcDriver());
        } catch ( SQLException ex ){
            ex.printStackTrace();
        }
    }


    /**
     * Connect to the database using a URL like :
     * jdbc:cassandra://host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[keyspace][?options]]
     * The URL excepting the jdbc: prefix is passed as it is to the Cassandra native Java driver.
     */
    public Connection connect(String url, Properties info) throws SQLException {
        if ( url != null && acceptsURL( url )){
            CassandraClientURI clientURI = new CassandraClientURI( url, info );
            Cluster cluster = clientURI.createBuilder();
            Session session = cluster.connect( clientURI.getDatabase() );

            return new CassandraConnection(session);
        }
        return null;
    }


    /**
     * URLs accepted are of the form: jdbc:cassandra://host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[keyspace][?options]]
     *
     * @see java.sql.Driver#acceptsURL(java.lang.String)
     */
    @Override
    public boolean acceptsURL(String url) {
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
