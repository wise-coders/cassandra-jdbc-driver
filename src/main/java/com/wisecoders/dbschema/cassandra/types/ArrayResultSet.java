package com.wisecoders.dbschema.cassandra.types;

import com.wisecoders.dbschema.cassandra.CassandraPreparedStatement;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;

/**
 * Licensed under <a href="https://creativecommons.org/licenses/by-nd/4.0/">CC BY-ND 4.0 DEED</a>, copyright <a href="https://wisecoders.com">Wise Coders GmbH</a>, used by <a href="https://dbschema.com">DbSchema Database Designer</a>.
 * Code modifications allowed only as pull requests to the <a href="https://github.com/wise-coders/cassandra-jdbc-driver">public GIT repository</a>.
 */

public class ArrayResultSet implements ResultSet
{
	private Object[][] data = null;

	private String[] columnNames = null;

	private int currentRow = -1;

	private String tableName = null;

	private boolean isClosed = false;

	private CassandraPreparedStatement statement = null;

	/**
	 * Public Constructor
	 */
	public ArrayResultSet()	{}

	public ArrayResultSet(String... columnNames)	{
		this.columnNames = columnNames;
	}

	public ArrayResultSet(Object[][] data, String[] columnNames)
	{
		if (data != null && data.length > 0 && data[0] != null)
		{
			int numRows = data.length;
			int numColumns = data[0].length;
			this.data = new Object[numRows][numColumns];
			for (int i = 0; i < numRows; i++)
			{
				this.data[i] = Arrays.copyOf(data[i], data[i].length);
			}

		}
		this.columnNames = columnNames;
	}

	/**
	 * Set the Result Set column names.
	 * @param columnNames
	 */
	public void setColumnNames(String[] columnNames) {
		this.columnNames = Arrays.copyOf(columnNames, columnNames.length);
	}

	/**
	 * Set the table used by this Result Set.
	 * @param tableName
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * Set the current statement used to generate this result set.
	 * @param statement used
	 */
	public void setStatement(CassandraPreparedStatement statement) {
		this.statement = statement;
	}

	public void addResultSet(ArrayResultSet toCopy)
	{
		if ( toCopy.data == null || toCopy.data.length < 0 ) {
			return;
		}
		if ( data == null )	{
			data = new Object[toCopy.data.length][toCopy.data[0].length];
			for (int i = 0; i < toCopy.data.length; i++ ) {
				data[i] = Arrays.copyOf(toCopy.data[i], toCopy.data[i].length);
			}
		} else {
			if (toCopy.data[0].length != data[0].length) {
				throw new IllegalArgumentException("Array toCopy column length (" + toCopy.data[0].length
						+ ") is not " + " the same as this result sets column length (" + toCopy.data[0].length + ")");
			}
			Object[][] newdata = new String[data.length + toCopy.data.length][data[0].length];
			for (int i = 0; i < data.length; i++) {
				newdata[i] = Arrays.copyOf(data[i], data[i].length);
			}
			for (int i = 0; i < toCopy.data.length; i++) {
				newdata[data.length+i] = Arrays.copyOf(toCopy.data[i], toCopy.data[i].length);
			}
			data = newdata;
		}
	}

	/**
	 * Add row to result set.
	 * @param columnValues
	 */
	public void addRow(Object[] columnValues)
	{
		if (data == null) {
			data = new Object[1][columnValues.length];
			data[0] = Arrays.copyOf(columnValues, columnValues.length);
		} else {
			int numRows = data.length;
			Object[][] newdata = new Object[numRows + 1][data[0].length];
			for (int i = 0; i < numRows; i++)
			{
				newdata[i] = Arrays.copyOf(data[i], data[i].length);
			}
			newdata[numRows] = Arrays.copyOf(columnValues, columnValues.length);
			data = newdata;
		}
	}

	/**
	 * @return number of rows
	 */
	public int getRowCount() {
		if (data == null) {
			return 0;
		}
		return data.length;
	}

	public <T> T unwrap(Class<T> iface) throws SQLException	{
		return null;
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException	{
		return false;
	}

	/**
	 * @see ResultSet#next()
	 */
	public boolean next() throws SQLException {
		if (data == null) {
			return false;
		}
		if (currentRow < data.length - 1) {
			currentRow++;
			return true;
		}
		return false;
	}

	/**
	 * @see ResultSet#close()
	 */
	public void close() throws SQLException	{
		this.isClosed = true;
	}

	/**
	 * @see ResultSet#wasNull()
	 */
	public boolean wasNull() throws SQLException {

		return false;
	}

	public String getString(int columnIndex) throws SQLException {
		if (currentRow >= data.length) {
			throw new SQLException("ResultSet exhausted, request currentRow = " + currentRow);
		}
		int adjustedColumnIndex = columnIndex - 1;
		if (adjustedColumnIndex >= data[currentRow].length)	{
			throw new SQLException("Column index does not exist: " + columnIndex);
		}
		final Object val = data[currentRow][adjustedColumnIndex];
		return val != null ? val.toString() : null;
	}

	public boolean getBoolean(int columnIndex) throws SQLException {
		return Boolean.parseBoolean(getString(columnIndex));
	}

	public byte getByte(int columnIndex) throws SQLException {
		return 0;
	}

	/**
	 * @see ResultSet#getShort(int)
	 */
	public short getShort(int columnIndex) throws SQLException {
		checkClosed();
		return Short.parseShort(getString(columnIndex));
	}

	/**
	 * @see ResultSet#getInt(int)
	 */
	public int getInt(int columnIndex) throws SQLException
	{
		checkClosed();
		return Integer.parseInt(getString(columnIndex));
	}

	/**
	 * @see ResultSet#getLong(int)
	 */
	public long getLong(int columnIndex) throws SQLException
	{
		checkClosed();
		return Long.parseLong(getString(columnIndex));
	}

	/**
	 * @see ResultSet#getFloat(int)
	 */
	public float getFloat(int columnIndex) throws SQLException
	{
		checkClosed();
		return Float.parseFloat(getString(columnIndex));
	}

	/**
	 * @see ResultSet#getDouble(int)
	 */
	public double getDouble(int columnIndex) throws SQLException
	{
		checkClosed();
		return Double.parseDouble(getString(columnIndex));
	}

	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException
	{

		return null;
	}

	public byte[] getBytes(int columnIndex) throws SQLException
	{

		return null;
	}

	public Date getDate(int columnIndex) throws SQLException
	{

		return null;
	}

	public Time getTime(int columnIndex) throws SQLException
	{

		return null;
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException
	{

		return null;
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException
	{

		return null;
	}

	public InputStream getUnicodeStream(int columnIndex) throws SQLException
	{

		return null;
	}

	public InputStream getBinaryStream(int columnIndex) throws SQLException
	{

		return null;
	}

	public String getString(String columnLabel) throws SQLException
	{
		checkClosed();
		int index = -1;
		if (columnNames == null) {
			throw new SQLException("Use of columnLabel requires setColumnNames to be called first.");
		}
		for (int i = 0; i < columnNames.length;  i++) {
			if (columnLabel.equals(columnNames[i])) {
				index = i;
				break;
			}
		}
		if (index == -1) {
			throw new SQLException("Column "+columnLabel+" doesn't exist in this ResultSet");
		}
		return getString(index+1);
	}

	public boolean getBoolean(String columnLabel) throws SQLException
	{
		checkClosed();

		return false;
	}

	public byte getByte(String columnLabel) throws SQLException
	{

		return 0;
	}

	public short getShort(String columnLabel) throws SQLException
	{

		return 0;
	}

	public int getInt(String columnLabel) throws SQLException
	{

		return 0;
	}

	public long getLong(String columnLabel) throws SQLException
	{

		return 0;
	}

	public float getFloat(String columnLabel) throws SQLException
	{

		return 0;
	}

	public double getDouble(String columnLabel) throws SQLException
	{
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
	 * @see ResultSet#getMetaData()
	 */
	public ResultSetMetaData getMetaData() throws SQLException
	{
		checkClosed();

		int[] columnDisplaySizes = new int[columnNames.length];
		int[] columnJavaTypes = new int[columnNames.length];
		for (int i = 0; i < columnDisplaySizes.length; i++) {
			columnDisplaySizes[i] = columnNames[i].length();
			columnJavaTypes[i] = Types.VARCHAR;
		}
		return new ResultSetMetaData() {
			@Override
			public int getColumnCount() throws SQLException {
				return columnNames.length;
			}

			@Override
			public boolean isAutoIncrement(int column) throws SQLException {
				return false;
			}

			@Override
			public boolean isCaseSensitive(int column) throws SQLException {
				return false;
			}

			@Override
			public boolean isSearchable(int column) throws SQLException {
				return false;
			}

			@Override
			public boolean isCurrency(int column) throws SQLException {
				return false;
			}

			@Override
			public int isNullable(int column) throws SQLException {
				return 0;
			}

			@Override
			public boolean isSigned(int column) throws SQLException {
				return false;
			}

			@Override
			public int getColumnDisplaySize(int column) throws SQLException {
				return columnDisplaySizes[column-1];
			}

			@Override
			public String getColumnLabel(int column) throws SQLException {
				return null;
			}

			@Override
			public String getColumnName(int column) throws SQLException {
				return columnNames[column-1];
			}

			@Override
			public String getSchemaName(int column) throws SQLException {
				return null;
			}

			@Override
			public int getPrecision(int column) throws SQLException {
				return 0;
			}

			@Override
			public int getScale(int column) throws SQLException {
				return 0;
			}

			@Override
			public String getTableName(int column) throws SQLException {
				return null;
			}

			@Override
			public String getCatalogName(int column) throws SQLException {
				return null;
			}

			@Override
			public int getColumnType(int column) throws SQLException {
				return columnJavaTypes[ column-1];
			}

			@Override
			public String getColumnTypeName(int column) throws SQLException {
				return null;
			}

			@Override
			public boolean isReadOnly(int column) throws SQLException {
				return false;
			}

			@Override
			public boolean isWritable(int column) throws SQLException {
				return false;
			}

			@Override
			public boolean isDefinitelyWritable(int column) throws SQLException {
				return false;
			}

			@Override
			public String getColumnClassName(int column) throws SQLException {
				return null;
			}

			@Override
			public <T> T unwrap(Class<T> iface) throws SQLException {
				return null;
			}

			@Override
			public boolean isWrapperFor(Class<?> iface) throws SQLException {
				return false;
			}
		};
	}

	public Object getObject(int columnIndex) throws SQLException {
		if (currentRow >= data.length)
		{
			throw new SQLException("ResultSet exhausted, request currentRow = " + currentRow);
		}
		int adjustedColumnIndex = columnIndex - 1;
		if (adjustedColumnIndex >= data[currentRow].length)
		{
			throw new SQLException("Column index does not exist: " + columnIndex);
		}
		return data[currentRow][adjustedColumnIndex];
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
	 * @see ResultSet#getType()
	 */
	public int getType() throws SQLException
	{
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	/**
	 * @see ResultSet#getConcurrency()
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
	 * @see ResultSet#getStatement()
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
	 * @see ResultSet#isClosed()
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
