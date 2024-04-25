package com.wisecoders.dbschema.cassandra;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.wisecoders.dbschema.cassandra.types.ArrayResultSet;
import com.wisecoders.dbschema.cassandra.types.BlindPreparedStatement;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Licensed under <a href="https://creativecommons.org/licenses/by-nd/4.0/">CC BY-ND 4.0 DEED</a>, copyright <a href="https://wisecoders.com">Wise Coders GmbH</a>, used by <a href="https://dbschema.com">DbSchema Database Designer</a>.
 * Code modifications allowed only as pull requests to the <a href="https://github.com/wise-coders/cassandra-jdbc-driver">public GIT repository</a>.
 */

public class CassandraConnection implements Connection {

    private final CqlSession session;
    private final JdbcDriver driver;
    private final boolean returnNullStringsFromIntroQuery;
    private boolean isClosed = false;
    private boolean isReadOnly = false;

    CassandraConnection(CqlSession session, JdbcDriver jdbcDriver, boolean returnNullStringsFromIntroQuery) {
        this.session = session;
        driver = jdbcDriver;
        this.returnNullStringsFromIntroQuery = returnNullStringsFromIntroQuery;
    }

    public String getCatalog() throws SQLException {
        checkClosed();
        try {
            return session.getKeyspace().toString();
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        }
    }

    private static final Pattern describeTable = Pattern.compile("DESC (.*)\\.(.*)", Pattern.CASE_INSENSITIVE);
    private static final Pattern describeKeyspace = Pattern.compile("DESC (.*)", Pattern.CASE_INSENSITIVE );


    public ResultSet executeDescribeCommand(String sql ){
        final ArrayResultSet rs = new ArrayResultSet("KEYSPACE", "CAT", "OBJECT", "DESC");
        {
            final Matcher matcher = describeTable.matcher(sql);
            if (matcher.matches()) {
                session.getMetadata().getKeyspace(matcher.group(1)).ifPresent(keyspaceMetadata -> {
                    keyspaceMetadata.getTable(matcher.group(2)).ifPresent(tableMetadata -> {
                        rs.addRow(new String[]{String.valueOf(keyspaceMetadata.getName()), null, matcher.group(1), tableMetadata.describeWithChildren(true)});
                    });
                });
                return rs;
            }
        }
        {
            final Matcher matcher = describeKeyspace.matcher(sql);
            if (matcher.matches()) {
                session.getMetadata().getKeyspace(matcher.group(1)).ifPresent(keyspaceMetadata -> {
                    rs.addRow(new String[]{String.valueOf(keyspaceMetadata.getName()), null, matcher.group(1), keyspaceMetadata.describeWithChildren(true)});
                });
                return rs;
            }
        }
        return null;
    }

    @SuppressWarnings("WeakerAccess")
    public CqlSession getSession() {
        return session;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        try {
            return new CassandraStatement(this);
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private static final String SELECT_COLUMNS_INTRO_QUERY = "SELECT column_name as name,\n       validator,\n       columnfamily_name as table_name,\n       type,\n       index_name,\n       index_options,\n       index_type,\n       component_index as position\nFROM system.schema_columns\nWHERE keyspace_name = ?";

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        try {
            return new CassandraPreparedStatement(session, session.prepare(sql), returnNullStringsFromIntroQuery || !SELECT_COLUMNS_INTRO_QUERY.equals(sql));
        } catch ( SyntaxError error ) {
            ResultSet rs = executeDescribeCommand( sql );
            if ( rs != null ){
                return new BlindPreparedStatement(rs );
            }
            throw new SQLSyntaxErrorException(error.getMessage(), error);
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        }
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Cassandra does not support SQL natively.");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return true;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new CassandraMetaData(this, driver);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        isReadOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return isReadOnly;
    }

    @Override
    public void setCatalog(String catalog) {

    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        // Since the only valid value for MongDB is Connection.TRANSACTION_NONE, and the javadoc for this method
        // indicates that this is not a valid value for level here, throw unsupported operation exception.
        throw new UnsupportedOperationException("Cassandra provides no support for transactions.");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }


    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        checkClosed();
        return true;
    }

    @Override
    public void setClientInfo(String name, String value) {
        /* Cassandra does not support setting client information in the database. */
    }

    @Override
    public void setClientInfo(Properties properties) {
        /* Cassandra does not support setting client information in the database. */
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkClosed();

        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkClosed();
        return null;
    }


    private void checkClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("Statement was previously closed.");
        }
    }

    @Override
    public void setSchema(String schema) {
        setCatalog(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return getCatalog();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


}
