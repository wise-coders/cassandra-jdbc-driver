
package com.dbschema;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.Session;
import com.dbschema.codec.jbytes.BlobCodec;
import com.dbschema.codec.jlong.*;
import com.dbschema.codec.jsqldate.DateCodec;
import com.dbschema.codec.jsqltime.TimeCodec;

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
            DriverManager.registerDriver(new CassandraJdbcDriver());
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }


    /**
     * Connect to the database using a URL like :
     * jdbc:cassandra://host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[keyspace][?options]]
     * The URL excepting the jdbc: prefix is passed as it is to the Cassandra native Java driver.
     */
    public Connection connect(String url, Properties info) throws SQLException {
        if (url != null && acceptsURL(url)) {
            CassandraClientURI clientURI = new CassandraClientURI(url, info);
            try {
                Cluster cluster = clientURI.createCluster();
                registerCodecs(cluster);
                String keyspace = clientURI.getKeyspace();
                if (!ParseUtils.isDoubleQuoted(keyspace)) {
                    keyspace = ParseUtils.doubleQuote(keyspace);
                }
                Session session = cluster.connect(keyspace);

                return new CassandraConnection(session, this);
            } catch (UnknownHostException e) {
                throw new SQLException(e.getMessage(), e);
            }
        }
        return null;
    }

    private void registerCodecs(Cluster cluster) {
        CodecRegistry myCodecRegistry = cluster.getConfiguration().getCodecRegistry();
        myCodecRegistry.register(IntCodec.INSTANCE);
        myCodecRegistry.register(DecimalCodec.INSTANCE);
        myCodecRegistry.register(DoubleCodec.INSTANCE);
        myCodecRegistry.register(com.dbschema.codec.jlong.FloatCodec.INSTANCE);
        myCodecRegistry.register(SmallintCodec.INSTANCE);
        myCodecRegistry.register(TinyintCodec.INSTANCE);
        myCodecRegistry.register(VarintCodec.INSTANCE);
        myCodecRegistry.register(BlobCodec.INSTANCE);
        myCodecRegistry.register(com.dbschema.codec.jdouble.FloatCodec.INSTANCE);
        myCodecRegistry.register(DateCodec.INSTANCE);
        myCodecRegistry.register(TimeCodec.INSTANCE);
        myCodecRegistry.register(com.dbschema.codec.juuid.StringCodec.INSTANCE);
        myCodecRegistry.register(com.dbschema.codec.jduration.StringCodec.INSTANCE);
        myCodecRegistry.register(com.dbschema.codec.jtimeuuid.StringCodec.INSTANCE);
        myCodecRegistry.register(com.dbschema.codec.jinet.StringCodec.INSTANCE);
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
        return "1.2.6-SNAPSHOT";
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 26;
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
