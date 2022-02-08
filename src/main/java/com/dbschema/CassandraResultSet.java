package com.dbschema;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.dbschema.CassandraResultSetMetaData.ColumnMetaData;
import com.dbschema.types.ArrayImpl;
import com.dbschema.types.BlobImpl;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

import static com.dbschema.DateUtil.Direction;
import static com.dbschema.DateUtil.considerTimeZone;

public class CassandraResultSet implements ResultSet {

    private boolean isClosed = false;

    private final Statement statement;
    private final com.datastax.oss.driver.api.core.cql.ResultSet dsResultSet;
    private final Iterator<Row> iterator;
    private final boolean returnNullStrings;
    private Row currentRow;

    CassandraResultSet(Statement statement, com.datastax.oss.driver.api.core.cql.ResultSet dsResultSet, boolean returnNullStrings) {
        this.statement = statement;
        this.dsResultSet = dsResultSet;
        this.iterator = dsResultSet.iterator();
        this.returnNullStrings = returnNullStrings;
    }

    CassandraResultSet(Statement statement, com.datastax.oss.driver.api.core.cql.ResultSet dsResultSet) {
        this(statement, dsResultSet, true);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean next() {
        if (iterator.hasNext()) {
            currentRow = iterator.next();
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        isClosed = true;
    }

    public boolean isQuery() {
        return dsResultSet.getColumnDefinitions().size() != 0;
    }

    @Override
    public boolean wasNull() {
        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow == null) throw new SQLException("Exhausted ResultSet.");
        Object o = currentRow.getObject(columnIndex - 1);
        return o == null ?
                returnNullStrings ? null : "" :
                String.valueOf(o);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getBool(columnIndex - 1);
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getByte(columnIndex - 1);
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getShort(columnIndex - 1);
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getInt(columnIndex - 1);
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getLong(columnIndex - 1);
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getFloat(columnIndex - 1);
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getDouble(columnIndex - 1);
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getBigDecimal(columnIndex - 1);
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            final ByteBuffer bytes = currentRow.getByteBuffer(columnIndex - 1);
            return bytes != null ? bytes.array() : null;
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            final LocalDate date = currentRow.getLocalDate(columnIndex - 1);
            return date != null ? Date.valueOf( date ) : null;
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            LocalTime time = currentRow.getLocalTime(columnIndex - 1);
            return Time.valueOf( time );
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            final LocalDateTime date = currentRow.getLocalDate(columnIndex - 1).atStartOfDay();
            return date != null ? Timestamp.valueOf( date ) : null;
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getString(columnLabel);
        }
        throw new SQLException("Result exhausted.");
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getBool(columnLabel);
        }
        throw new SQLException("Result exhausted.");
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getByte(columnLabel);
        }
        throw new SQLException("Result exhausted.");
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getShort(columnLabel);
        }
        throw new SQLException("Result exhausted.");
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getInt(columnLabel);
        }
        throw new SQLException("Result exhausted.");
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getLong(columnLabel);
        }
        throw new SQLException("Result exhausted.");
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getFloat(columnLabel);
        }
        throw new SQLException("Result exhausted.");
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getDouble(columnLabel);
        }
        throw new SQLException("Result exhausted.");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            ByteBuffer bytes = currentRow.getByteBuffer(columnLabel);
            return bytes == null ? null : bytes.array();
        }
        throw new SQLException("Result exhausted.");
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() {
        // SUGGESTED BY CRISTI TO SHOW EXECUTION WARNINGS
        StringBuilder sb = new StringBuilder();
        for (String warning : dsResultSet.getExecutionInfo().getWarnings()) {
            sb.append(warning).append(" ");
        }
        return sb.length() > 0 ? new SQLWarning(sb.toString()) : null;
    }

    @Override
    public void clearWarnings() {
        // todo
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        dsResultSet.getColumnDefinitions();
        List<ColumnMetaData> columnMetaData = new ArrayList<>();
        for (Iterator<ColumnDefinition> itr = dsResultSet.getColumnDefinitions().iterator(); itr.hasNext(); ) {
            ColumnDefinition def = itr.next();
            columnMetaData.add(new ColumnMetaData(def.getName().toString(), def.getTable().toString(), def.getKeyspace().toString(), def.getType().toString() ));
        }
        return new CassandraResultSetMetaData(columnMetaData);
    }


    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getObject(columnIndex - 1);
        }
        throw new SQLException("Result exhausted.");
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            return currentRow.getObject(columnLabel);
        }
        throw new SQLException("Result exhausted.");
    }

    @Override
    public int findColumn(String columnLabel) {
        return dsResultSet.getColumnDefinitions().firstIndexOf(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) {
        // todo
    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public int getType() {
        return ResultSet.TYPE_FORWARD_ONLY; // todo
    }

    @Override
    public int getConcurrency() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Statement getStatement() {
        return this.statement;
    }

    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            ByteBuffer bytes = currentRow.getByteBuffer(columnIndex - 1);
            return bytes == null ? null : new BlobImpl(bytes.array());
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLException("Clob type is not supported by Cassandra");
    }

    public Array getArray(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            Object o = currentRow.getObject(columnIndex - 1);
            if (!(o instanceof List)) return null;
            List list = (List) o;
            return toArray(list);
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    private Array toArray(List list) {
        Object[] array = new Object[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return new ArrayImpl(array);
    }

    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Blob getBlob(String columnLabel) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            ByteBuffer bytes = currentRow.getByteBuffer(columnLabel);
            return bytes == null ? null : new BlobImpl(bytes.array());
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLException("Clob type is not supported by Cassandra");
    }

    public Array getArray(String columnLabel) throws SQLException {
        checkClosed();
        if (currentRow != null) {
            Object o = currentRow.getObject(columnLabel);
            if (!(o instanceof List)) return null;
            List list = (List) o;
            return toArray(list);
        }
        throw new SQLException("Exhausted ResultSet.");
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return considerTimeZone(getDate(columnIndex), cal, Direction.FROM_UTC);
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return considerTimeZone(getTime(columnIndex), cal, Direction.FROM_UTC);
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return DateUtil.considerTimeZone(getTimestamp(columnIndex), cal, Direction.FROM_UTC);
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private void checkClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("ResultSet was previously closed.");
        }
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
