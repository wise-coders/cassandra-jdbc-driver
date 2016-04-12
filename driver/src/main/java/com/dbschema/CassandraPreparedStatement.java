
package com.dbschema;

import com.datastax.driver.core.BoundStatement;
import com.dbschema.resultSet.ResultSetWrapper;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;

public class CassandraPreparedStatement implements PreparedStatement {

    private final CassandraConnection connection;
    private ResultSet lastResultSet;
    private boolean isClosed = false;
    private int maxRows = -1;
    private final String sql;
    private ArrayList<Object> params;

    public CassandraPreparedStatement(final CassandraConnection connection) {
        this.connection = connection;
        this.sql = null;
    }

    public CassandraPreparedStatement(final CassandraConnection connection, String sql) {
        this.connection = connection;
        this.sql = sql;
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException	{
        checkClosed();
        if (lastResultSet != null ) {
            lastResultSet.close();
        }
        if ( sql == null ){
            throw new SQLException("Null statement.");
        }
        ResultSet rs;
        if ( params != null ){
            com.datastax.driver.core.PreparedStatement dsps = connection.session.prepare( sql );
            BoundStatement boundStatement = new BoundStatement(dsps);
            rs = new ResultSetWrapper( this, connection.session.execute( boundStatement.bind( params.toArray(new Object[params.size()]) )));
            params.clear();
        } else {
            rs = new ResultSetWrapper( this, connection.session.execute( sql ) );
        }
        return rs;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery( sql );
    }

    @Override
    public boolean execute(final String sql) throws SQLException {
        executeQuery( sql );
        return lastResultSet != null;
    }

    @Override
    public void setObject(int parameterIndex, Object value) throws SQLException {
        if ( params == null ){
            params = new ArrayList<Object>();
        }
        int idx = parameterIndex-1;
        for ( int i = params.size(); i < idx; i++){
            params.add( i, null );
        }
        params.add(idx, value);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return executeUpdate( sql );
    }

    @Override
    public int executeUpdate( String sql) throws SQLException	{
        executeQuery(sql);
        return 1;
    }

    @Override
    public void close() throws SQLException	{
        if (lastResultSet != null) {
            lastResultSet.close();
        }
        this.isClosed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException
    {

        return 0;
    }

    @Override
    public void setMaxFieldSize(final int max) throws SQLException{	}

    @Override
    public int getMaxRows() throws SQLException	{
        return maxRows;
    }

    @Override
    public void setMaxRows(final int max) throws SQLException
    {
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(final boolean enable) throws SQLException{}

    @Override
    public int getQueryTimeout() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("MongoDB provides no support for query timeouts.");
    }

    @Override
    public void setQueryTimeout(final int seconds) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("MongoDB provides no support for query timeouts.");
    }

    @Override
    public void cancel() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("MongoDB provides no support for interrupting an operation.");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException	{
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException	{
        checkClosed();
    }

    @Override
    public void setCursorName(final String name) throws SQLException {
        checkClosed();
        // Driver doesn't support positioned updates for now, so no-op.
    }

    @Override
    public ResultSet getResultSet() throws SQLException	{
        checkClosed();
        return lastResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException	{
        checkClosed();
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(final int direction) throws SQLException{}

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(final int rows) throws SQLException{}

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(final String sql) throws SQLException{}

    @Override
    public void clearBatch() throws SQLException{}

    @Override
    public int[] executeBatch() throws SQLException	{
        checkClosed();
        return null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return this.connection;
    }

    @Override
    public boolean getMoreResults(final int current) throws SQLException
    {
        checkClosed();
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException	{
        checkClosed();
        return null;
    }

    @Override
    public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException	{
        checkClosed();
        return 0;
    }

    @Override
    public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public int executeUpdate(final String sql, final String[] columnNames) throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public boolean execute(final String sql, final int[] columnIndexes) throws SQLException	{
        checkClosed();
        return false;
    }

    @Override
    public boolean execute(final String sql, final String[] columnNames) throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public void setPoolable(final boolean poolable) throws SQLException	{}

    @Override
    public boolean isPoolable() throws SQLException	{
        return false;
    }

    private void checkClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("Statement was previously closed.");
        }
    }

    @Override
    public void closeOnCompletion() throws SQLException {
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void clearParameters() throws SQLException {
        params = null;
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    }


    @Override
    public boolean execute() throws SQLException {
        return false;
    }

    @Override
    public void addBatch() throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {

    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {

    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {

    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject( parameterIndex, x );
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {

    }
}


