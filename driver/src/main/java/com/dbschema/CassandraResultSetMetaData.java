package com.dbschema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class CassandraResultSetMetaData implements ResultSetMetaData {

    private final String tableName;
    private final String[] columnNames;
    private final int[] javaTypes;
    private final int[] displaySizes;

    public CassandraResultSetMetaData(String tableName, String[] columnNames, int[] javaTypes, int[] displaySizes) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.displaySizes = displaySizes;
        this.javaTypes = javaTypes;
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
    public int getColumnCount() {
        return this.columnNames.length;
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int isNullable(int column) {
        return ResultSetMetaData.columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return displaySizes[column - 1];
    }

    @Override
    public String getColumnLabel(int column) {
        return columnNames[column - 1];
    }

    @Override
    public String getColumnName(int column) {
        return columnNames[column - 1];
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return getCatalogName(column);
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getScale(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getTableName(int column) {
        return tableName;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getColumnType(int column) {
        return javaTypes[column - 1];
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

}
