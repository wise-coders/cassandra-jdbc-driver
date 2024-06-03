
package com.wisecoders.dbschema.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.*;

import static com.wisecoders.dbschema.cassandra.CassandraClientURI.PREFIX;


/**
 * Minimal implementation of the JDBC standards for the Cassandra database.
 * This is customized for DbSchema database designer.
 * Connect to the database using a URL like :
 * jdbc:cassandra://host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[keyspace][?options]]
 * The URL excepting the jdbc: prefix is passed as it is to the Cassandra native Java driver.
 *
 * Licensed under <a href="https://creativecommons.org/licenses/by-nd/4.0/">CC BY-ND 4.0 DEED</a>, copyright <a href="https://wisecoders.com">Wise Coders GmbH</a>, used by <a href="https://dbschema.com">DbSchema Database Designer</a>.
 * Code modifications allowed only as pull requests to the <a href="https://github.com/wise-coders/cassandra-jdbc-driver">public GIT repository</a>.
 */


public class JdbcDriver implements Driver {
    private static final String RETURN_NULL_STRINGS_FROM_INTRO_QUERY_KEY = "cassandra.jdbc.return.null.strings.from.intro.query";

    public static final Logger LOGGER = Logger.getLogger( JdbcDriver.class.getName() );

    static {
        try {
            final File logsFile = new File("~/.DbSchema/logs/");
            if ( !logsFile.exists()) {
                logsFile.mkdirs();
            }

            DriverManager.registerDriver( new JdbcDriver());
            LOGGER.setLevel(Level.ALL);
            final ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(Level.ALL);
            consoleHandler.setFormatter(new SimpleFormatter());
            LOGGER.addHandler(consoleHandler);

            final FileHandler fileHandler = new FileHandler(System.getProperty("user.home") + "/.DbSchema/logs/CassandraJdbcDriver.log");
            fileHandler.setFormatter( new SimpleFormatter());
            fileHandler.setLevel(Level.ALL);
            LOGGER.addHandler(fileHandler);
        } catch ( Exception ex ){
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
                CqlSession session = clientURI.createCqlSession();
                try {
                    session.execute("SELECT cql_version FROM system.local");
                } catch (Throwable e) {
                    throw new SQLException(e.getMessage(), e);
                }
                boolean returnNullStringsFromIntroQuery = Boolean.parseBoolean( info.getProperty( RETURN_NULL_STRINGS_FROM_INTRO_QUERY_KEY ) );
                return new CassandraConnection(session, this, returnNullStringsFromIntroQuery);
            } catch (UnknownHostException e) {
                throw new SQLException(e.getMessage(), e);
            } catch (GeneralSecurityException e) {
                throw new SQLException(e.getMessage(), e);
            } catch (IOException e) {
                throw new SQLException(e.getMessage(), e);
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
        return "1.3.4";
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 3;
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
