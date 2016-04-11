package com.dbs.resultSet;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.dbs.CassandraResultSetMetaData;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;

public class DxResultSet implements ResultSet
{

    public static final int TYPE_MAP = 4999544;
    public static final int TYPE_LIST = 4999545;

	private boolean isClosed = false;

    private final Statement statement;
    public final com.datastax.driver.core.ResultSet dsResultSet;
    private final Iterator<Row> iterator;
    private Row currentRow;

	public DxResultSet( Statement statement, com.datastax.driver.core.ResultSet dsResultSet )
	{
        this.statement = statement;
		this.dsResultSet = dsResultSet;
        this.iterator = dsResultSet.iterator();
	}



	public <T> T unwrap(Class<T> iface) throws SQLException {
		return null;
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#next()
	 */
	public boolean next() throws SQLException {
        if ( iterator.hasNext() ) {
            currentRow = iterator.next();
            return true;
        }
		return false;
	}

	/**
	 * @see java.sql.ResultSet#close()
	 */
	public void close() throws SQLException {
        isClosed = true;
	}

	/**
	 * @see java.sql.ResultSet#wasNull()
	 */
	public boolean wasNull() throws SQLException {
		return false;
	}

	public String getString(int columnIndex) throws SQLException {
        checkClosed();
        if ( currentRow != null ){
            return currentRow.getString( columnIndex );
        }
        throw new SQLException("Exhausted ResultSet.");
	}

	public boolean getBoolean(int columnIndex) throws SQLException{
        checkClosed();
        if ( currentRow != null ){
            return currentRow.getBool(columnIndex);
        }
        throw new SQLException("Exhausted ResultSet.");
	}

	public byte getByte(int columnIndex) throws SQLException {
        checkClosed();
        if ( currentRow != null ){
            return currentRow.getByte(columnIndex);
        }
        throw new SQLException("Exhausted ResultSet.");
	}

	/**
	 * @see java.sql.ResultSet#getShort(int)
	 */
	public short getShort(int columnIndex) throws SQLException {
        checkClosed();
        if ( currentRow != null ){
            return currentRow.getShort(columnIndex);
        }
        throw new SQLException("Exhausted ResultSet.");
	}

	/**
	 * @see java.sql.ResultSet#getInt(int)
	 */
	public int getInt(int columnIndex) throws SQLException {
        checkClosed();
        if ( currentRow != null ){
            return currentRow.getInt(columnIndex);
        }
        throw new SQLException("Exhausted ResultSet.");
	}

	/**
	 * @see java.sql.ResultSet#getLong(int)
	 */
	public long getLong(int columnIndex) throws SQLException {
        checkClosed();
        if ( currentRow != null ){
            return currentRow.getLong(columnIndex);
        }
        throw new SQLException("Exhausted ResultSet.");
	}

	/**
	 * @see java.sql.ResultSet#getFloat(int)
	 */
	public float getFloat(int columnIndex) throws SQLException {
        checkClosed();
        if ( currentRow != null ){
            return currentRow.getFloat(columnIndex);
        }
        throw new SQLException("Exhausted ResultSet.");
	}

	/**
	 * @see java.sql.ResultSet#getDouble(int)
	 */
	public double getDouble(int columnIndex) throws SQLException {
        checkClosed();
        if ( currentRow != null ){
            return currentRow.getDouble(columnIndex);
        }
        throw new SQLException("Exhausted ResultSet.");
	}

	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException	{
        checkClosed();
        if ( currentRow != null ){
            return currentRow.getDecimal(columnIndex);
        }
        throw new SQLException("Exhausted ResultSet.");
	}

	public byte[] getBytes(int columnIndex) throws SQLException	{
        checkClosed();
        if ( currentRow != null ){
            return currentRow.getBytes( columnIndex ).array();
        }
        throw new SQLException("Exhausted ResultSet.");
	}

	public Date getDate(int columnIndex) throws SQLException {
        checkClosed();
        if ( currentRow != null ){
            return new Date( currentRow.getDate( columnIndex ).getMillisSinceEpoch() );
        }
        throw new SQLException("Exhausted ResultSet.");
	}

	public Time getTime(int columnIndex) throws SQLException {
        checkClosed();
        if ( currentRow != null ){
            return new Time( currentRow.getDate( columnIndex ).getMillisSinceEpoch() );
        }
        throw new SQLException("Exhausted ResultSet.");
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkClosed();
        if ( currentRow != null ){
            return new Timestamp( currentRow.getDate( columnIndex ).getMillisSinceEpoch() );
        }
        throw new SQLException("Exhausted ResultSet.");
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException	{
        return null;
	}

	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		return null;
	}

	public InputStream getBinaryStream(int columnIndex) throws SQLException	{
		return null;
	}

	public String getString(String columnLabel) throws SQLException	{
        checkClosed();
        if ( currentRow != null ){
            return currentRow.getString(columnLabel);
        }
        throw new SQLException("Column '" + columnLabel + "' not found.");
	}

	public boolean getBoolean(String columnLabel) throws SQLException {
        return false;
	}

	public byte getByte(String columnLabel) throws SQLException	{
		return 0;
	}

	public short getShort(String columnLabel) throws SQLException{
		return 0;
	}

	public int getInt(String columnLabel) throws SQLException {
		return 0;
	}

	public long getLong(String columnLabel) throws SQLException	{
		return 0;
	}

	public float getFloat(String columnLabel) throws SQLException {
		return 0;
	}

	public double getDouble(String columnLabel) throws SQLException	{
		return Double.parseDouble(getString(columnLabel));
	}

	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException
	{

		return null;
	}

	public byte[] getBytes(String columnLabel) throws SQLException
	{
		return getString(columnLabel).getBytes();
	}

	public Date getDate(String columnLabel) throws SQLException
	{

		return null;
	}

	public Time getTime(String columnLabel) throws SQLException
	{

		return null;
	}

	public Timestamp getTimestamp(String columnLabel) throws SQLException
	{

		return null;
	}

	public InputStream getAsciiStream(String columnLabel) throws SQLException
	{

		return null;
	}

	public InputStream getUnicodeStream(String columnLabel) throws SQLException	{
		return null;
	}

	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		return null;
	}

	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	public void clearWarnings() throws SQLException
	{


	}

	public String getCursorName() throws SQLException
	{

		return null;
	}

	/**
	 * @see java.sql.ResultSet#getMetaData()
	 */
	public ResultSetMetaData getMetaData() throws SQLException
	{
		checkClosed();
        int size = dsResultSet.getColumnDefinitions().size();
        String[] columnNames = new String[size];
        int[] columnDisplaySizes = new int[size];
        int[] columnJavaTypes = new int[size];
        String tableName = null;
        int i = 0;
        for ( ColumnDefinitions.Definition def : dsResultSet.getColumnDefinitions() ){
            columnNames[i] = def.getName();
            int type = Types.VARCHAR;
            String typeName = def.getType().getName().name();
            if ( "ascii".equalsIgnoreCase( typeName ))type = Types.VARCHAR;
            else if ( "bigint".equalsIgnoreCase( typeName ))type = Types.BIGINT;
            else if ( "blob".equalsIgnoreCase( typeName ))type = Types.BLOB;
            else if ( "boolean".equalsIgnoreCase( typeName ))type = Types.BOOLEAN;
            else if ( "counter".equalsIgnoreCase( typeName ))type = Types.NUMERIC;
            else if ( "decimal".equalsIgnoreCase( typeName ))type = Types.DECIMAL;
            else if ( "double".equalsIgnoreCase( typeName ))type = Types.DOUBLE;
            else if ( "float".equalsIgnoreCase( typeName ))type = Types.FLOAT;
            else if ( "inet".equalsIgnoreCase( typeName ))type = Types.VARCHAR;
            else if ( "int".equalsIgnoreCase( typeName ))type = Types.INTEGER;
            else if ( "list".equalsIgnoreCase( typeName ))type = TYPE_LIST;
            else if ( "map".equalsIgnoreCase( typeName ))type = TYPE_MAP;
            else if ( "set".equalsIgnoreCase( typeName ))type = Types.STRUCT;
            else if ( "text".equalsIgnoreCase( typeName ))type = Types.VARCHAR;
            else if ( "timestamp".equalsIgnoreCase( typeName ))type = Types.TIMESTAMP;
            else if ( "uuid".equalsIgnoreCase( typeName ))type = Types.ROWID;
            else if ( "timesuuid".equalsIgnoreCase( typeName ))type = Types.ROWID;
            else if ( "varchar".equalsIgnoreCase( typeName ))type = Types.VARCHAR;
            else if ( "varint".equalsIgnoreCase( typeName ))type = Types.INTEGER;

            columnDisplaySizes[i] = 100;
            columnJavaTypes[i] = type;
            tableName = def.getTable();
            i++;
        }

		return new CassandraResultSetMetaData(tableName, columnNames, columnJavaTypes, columnDisplaySizes);
	}

	public Object getObject(int columnIndex) throws SQLException
	{

		return null;
	}

	public Object getObject(String columnLabel) throws SQLException
	{

		return null;
	}

	public int findColumn(String columnLabel) throws SQLException
	{

		return 0;
	}

	public Reader getCharacterStream(int columnIndex) throws SQLException
	{

		return null;
	}

	public Reader getCharacterStream(String columnLabel) throws SQLException
	{

		return null;
	}

	public BigDecimal getBigDecimal(int columnIndex) throws SQLException
	{

		return null;
	}

	public BigDecimal getBigDecimal(String columnLabel) throws SQLException
	{

		return null;
	}

	public boolean isBeforeFirst() throws SQLException
	{

		return false;
	}

	public boolean isAfterLast() throws SQLException
	{

		return false;
	}

	public boolean isFirst() throws SQLException
	{

		return false;
	}

	public boolean isLast() throws SQLException
	{

		return false;
	}

	public void beforeFirst() throws SQLException
	{


	}

	public void afterLast() throws SQLException
	{


	}

	public boolean first() throws SQLException
	{

		return false;
	}

	public boolean last() throws SQLException
	{

		return false;
	}

	public int getRow() throws SQLException
	{

		return 0;
	}

	public boolean absolute(int row) throws SQLException
	{

		return false;
	}

	public boolean relative(int rows) throws SQLException
	{

		return false;
	}

	public boolean previous() throws SQLException
	{

		return false;
	}

	public void setFetchDirection(int direction) throws SQLException
	{


	}

	public int getFetchDirection() throws SQLException
	{

		return 0;
	}

	public void setFetchSize(int rows) throws SQLException
	{


	}

	public int getFetchSize() throws SQLException
	{

		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getType()
	 */
	public int getType() throws SQLException
	{
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	/**
	 * @see java.sql.ResultSet#getConcurrency()
	 */
	public int getConcurrency() throws SQLException
	{

		return 0;
	}

	public boolean rowUpdated() throws SQLException
	{

		return false;
	}

	public boolean rowInserted() throws SQLException
	{

		return false;
	}

	public boolean rowDeleted() throws SQLException
	{

		return false;
	}

	public void updateNull(int columnIndex) throws SQLException
	{


	}

	public void updateBoolean(int columnIndex, boolean x) throws SQLException
	{


	}

	public void updateByte(int columnIndex, byte x) throws SQLException
	{


	}

	public void updateShort(int columnIndex, short x) throws SQLException
	{


	}

	public void updateInt(int columnIndex, int x) throws SQLException
	{


	}

	public void updateLong(int columnIndex, long x) throws SQLException
	{


	}

	public void updateFloat(int columnIndex, float x) throws SQLException
	{


	}

	public void updateDouble(int columnIndex, double x) throws SQLException
	{


	}

	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException
	{


	}

	public void updateString(int columnIndex, String x) throws SQLException
	{


	}

	public void updateBytes(int columnIndex, byte[] x) throws SQLException
	{


	}

	public void updateDate(int columnIndex, Date x) throws SQLException
	{


	}

	public void updateTime(int columnIndex, Time x) throws SQLException
	{


	}

	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException
	{


	}

	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException
	{


	}

	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException
	{


	}

	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException
	{


	}

	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException
	{


	}

	public void updateObject(int columnIndex, Object x) throws SQLException
	{


	}

	public void updateNull(String columnLabel) throws SQLException
	{


	}

	public void updateBoolean(String columnLabel, boolean x) throws SQLException
	{


	}

	public void updateByte(String columnLabel, byte x) throws SQLException
	{


	}

	public void updateShort(String columnLabel, short x) throws SQLException
	{


	}

	public void updateInt(String columnLabel, int x) throws SQLException
	{


	}

	public void updateLong(String columnLabel, long x) throws SQLException
	{


	}

	public void updateFloat(String columnLabel, float x) throws SQLException
	{


	}

	public void updateDouble(String columnLabel, double x) throws SQLException
	{


	}

	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException
	{


	}

	public void updateString(String columnLabel, String x) throws SQLException
	{


	}

	public void updateBytes(String columnLabel, byte[] x) throws SQLException
	{


	}

	public void updateDate(String columnLabel, Date x) throws SQLException
	{


	}

	public void updateTime(String columnLabel, Time x) throws SQLException
	{


	}

	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException
	{


	}

	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException
	{


	}

	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException
	{


	}

	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException
	{


	}

	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException
	{


	}

	public void updateObject(String columnLabel, Object x) throws SQLException
	{


	}

	public void insertRow() throws SQLException
	{


	}

	public void updateRow() throws SQLException
	{


	}

	public void deleteRow() throws SQLException
	{


	}

	public void refreshRow() throws SQLException
	{


	}

	public void cancelRowUpdates() throws SQLException
	{


	}

	public void moveToInsertRow() throws SQLException
	{


	}

	public void moveToCurrentRow() throws SQLException
	{


	}

	/**
	 * @see java.sql.ResultSet#getStatement()
	 */
	public Statement getStatement() throws SQLException
	{
		return this.statement;
	}

	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException
	{

		return null;
	}

	public Ref getRef(int columnIndex) throws SQLException
	{

		return null;
	}

	public Blob getBlob(int columnIndex) throws SQLException
	{

		return null;
	}

	public Clob getClob(int columnIndex) throws SQLException
	{

		return null;
	}

	public Array getArray(int columnIndex) throws SQLException
	{

		return null;
	}

	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException
	{

		return null;
	}

	public Ref getRef(String columnLabel) throws SQLException
	{

		return null;
	}

	public Blob getBlob(String columnLabel) throws SQLException
	{

		return null;
	}

	public Clob getClob(String columnLabel) throws SQLException
	{

		return null;
	}

	public Array getArray(String columnLabel) throws SQLException
	{

		return null;
	}

	public Date getDate(int columnIndex, Calendar cal) throws SQLException
	{

		return null;
	}

	public Date getDate(String columnLabel, Calendar cal) throws SQLException
	{

		return null;
	}

	public Time getTime(int columnIndex, Calendar cal) throws SQLException
	{

		return null;
	}

	public Time getTime(String columnLabel, Calendar cal) throws SQLException
	{

		return null;
	}

	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException
	{

		return null;
	}

	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException
	{

		return null;
	}

	public URL getURL(int columnIndex) throws SQLException
	{

		return null;
	}

	public URL getURL(String columnLabel) throws SQLException
	{

		return null;
	}

	public void updateRef(int columnIndex, Ref x) throws SQLException
	{


	}

	public void updateRef(String columnLabel, Ref x) throws SQLException
	{


	}

	public void updateBlob(int columnIndex, Blob x) throws SQLException
	{


	}

	public void updateBlob(String columnLabel, Blob x) throws SQLException
	{


	}

	public void updateClob(int columnIndex, Clob x) throws SQLException
	{


	}

	public void updateClob(String columnLabel, Clob x) throws SQLException
	{


	}

	public void updateArray(int columnIndex, Array x) throws SQLException
	{


	}

	public void updateArray(String columnLabel, Array x) throws SQLException
	{


	}

	public RowId getRowId(int columnIndex) throws SQLException
	{

		return null;
	}

	public RowId getRowId(String columnLabel) throws SQLException
	{

		return null;
	}

	public void updateRowId(int columnIndex, RowId x) throws SQLException
	{


	}

	public void updateRowId(String columnLabel, RowId x) throws SQLException
	{


	}

	public int getHoldability() throws SQLException
	{

		return 0;
	}

	/**
	 * @see java.sql.ResultSet#isClosed()
	 */
	public boolean isClosed() throws SQLException
	{
		return isClosed;
	}

	public void updateNString(int columnIndex, String nString) throws SQLException
	{
		checkClosed();


	}

	public void updateNString(String columnLabel, String nString) throws SQLException
	{


	}

	public void updateNClob(int columnIndex, NClob nClob) throws SQLException
	{


	}

	public void updateNClob(String columnLabel, NClob nClob) throws SQLException
	{


	}

	public NClob getNClob(int columnIndex) throws SQLException
	{

		return null;
	}

	public NClob getNClob(String columnLabel) throws SQLException
	{

		return null;
	}

	public SQLXML getSQLXML(int columnIndex) throws SQLException
	{

		return null;
	}

	public SQLXML getSQLXML(String columnLabel) throws SQLException
	{

		return null;
	}

	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException
	{


	}

	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException
	{


	}

	public String getNString(int columnIndex) throws SQLException
	{

		return null;
	}

	public String getNString(String columnLabel) throws SQLException
	{

		return null;
	}

	public Reader getNCharacterStream(int columnIndex) throws SQLException
	{

		return null;
	}

	public Reader getNCharacterStream(String columnLabel) throws SQLException
	{

		return null;
	}

	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException
	{


	}

	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
	{


	}

	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException
	{


	}

	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException
	{


	}

	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException
	{


	}

	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException
	{


	}

	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException
	{


	}

	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
	{


	}

	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException
	{


	}

	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException
	{


	}

	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException
	{


	}

	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException
	{


	}

	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException
	{


	}

	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException
	{


	}

	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException
	{


	}

	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException
	{


	}

	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException
	{


	}

	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException
	{


	}

	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException
	{


	}

	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException
	{


	}

	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException
	{


	}

	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException
	{


	}

	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException
	{


	}

	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException
	{


	}

	public void updateClob(int columnIndex, Reader reader) throws SQLException
	{


	}

	public void updateClob(String columnLabel, Reader reader) throws SQLException
	{


	}

	public void updateNClob(int columnIndex, Reader reader) throws SQLException
	{


	}

	public void updateNClob(String columnLabel, Reader reader) throws SQLException
	{


	}

	private void checkClosed() throws SQLException
	{
		if (isClosed) {
			throw new SQLException("ResultSet was previously closed.");
		}
	}

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return null;
    }
}
