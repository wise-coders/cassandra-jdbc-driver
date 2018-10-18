
package com.dbschema;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.net.UnknownHostException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

import static com.dbschema.CassandraClientURI.PREFIX;


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
            try	{

                Cluster cluster = clientURI.createCluster();
                Session session = cluster.connect( clientURI.getKeyspace() );

                return new CassandraConnection(session, this);
            } catch (UnknownHostException e) {
                throw new SQLException( e.getMessage(), e);
            }
        }
        return null;
    }


    /**
     * URLs accepted are of the form: jdbc:cassandra://host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[keyspace][?options]]
     */
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith(PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return null;
    }

    String getVersion() {
        return "1.2.3-SNAPSHOT";
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 23;
    }

    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

}
