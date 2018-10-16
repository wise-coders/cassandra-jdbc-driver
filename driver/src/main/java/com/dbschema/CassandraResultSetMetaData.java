package com.dbschema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraResultSetMetaData implements ResultSetMetaData {

    private final List<ColumnMetaData> columnMetaData;

    public CassandraResultSetMetaData(List<ColumnMetaData> columnMetaData) {
        this.columnMetaData = columnMetaData;
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
        return this.columnMetaData.size();
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
    public int getColumnDisplaySize(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getColumnLabel(int column) {
        return columnMetaData.get(column - 1).name;
    }

    @Override
    public String getColumnName(int column) {
        return columnMetaData.get(column - 1).name;
    }

    @Override
    public String getSchemaName(int column) {
        return getCatalogName(column - 1);
    }

    @Override
    public int getPrecision(int column) {
        return 0; // todo
    }

    @Override
    public int getScale(int column) {
        return 0; // todo
    }

    @Override
    public String getTableName(int column) {
        return columnMetaData.get(column - 1).tableName;
    }

    @Override
    public String getCatalogName(int column) {
        return columnMetaData.get(column - 1).keyspace;
    }

    @Override
    public int getColumnType(int column) {
        return columnMetaData.get(column - 1).getJavaType();
    }

    @Override
    public String getColumnTypeName(int column) {
        return columnMetaData.get(column - 1).typeName;
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
    public String getColumnClassName(int column) {
        return columnMetaData.get(column - 1).getClassName();
    }

    public static class ColumnMetaData {
        private static final int TYPE_MAP = 4999544;
        private static final int TYPE_LIST = 4999545;

        private static final Map<String, Integer> javaTypeMap = new HashMap<>();
        private static final Map<String, String> typeNameMap = new HashMap<>();

        static {
            javaTypeMap.put("ascii", Types.VARCHAR);
            javaTypeMap.put("bigint", Types.BIGINT);
            javaTypeMap.put("blob", Types.BLOB);
            javaTypeMap.put("boolean", Types.BOOLEAN);
            javaTypeMap.put("counter", Types.NUMERIC);
            javaTypeMap.put("decimal", Types.DECIMAL);
            javaTypeMap.put("double", Types.DOUBLE);
            javaTypeMap.put("float", Types.FLOAT);
            javaTypeMap.put("inet", Types.VARCHAR);
            javaTypeMap.put("int", Types.INTEGER);
            javaTypeMap.put("list", TYPE_LIST);
            javaTypeMap.put("map", TYPE_MAP);
            javaTypeMap.put("set", Types.STRUCT);
            javaTypeMap.put("text", Types.VARCHAR);
            javaTypeMap.put("timestamp", Types.TIMESTAMP);
            javaTypeMap.put("uuid", Types.ROWID);
            javaTypeMap.put("timesuuid", Types.ROWID);
            javaTypeMap.put("varchar", Types.VARCHAR);
            javaTypeMap.put("varint", Types.INTEGER);

            typeNameMap.put("ascii", "java.lang.String");
            typeNameMap.put("bigint", "java.lang.Long");
            typeNameMap.put("blob", "java.lang.Byte[]");
            typeNameMap.put("boolean", "java.lang.Boolean");
            typeNameMap.put("counter", "java.lang.Integer");
            typeNameMap.put("decimal", "java.math.BigDecimal");
            typeNameMap.put("double", "java.lang.Double");
            typeNameMap.put("float", "java.lang.Float");
            typeNameMap.put("inet", "java.lang.String");
            typeNameMap.put("int", "java.lang.Integer");
            typeNameMap.put("list", "java.util.List");
            typeNameMap.put("map", "java.util.Map");
            typeNameMap.put("set", "java.lang.Set");
            typeNameMap.put("text", "java.lang.String");
            typeNameMap.put("timestamp", "java.util.Date");
            typeNameMap.put("uuid", "java.util.String");
            typeNameMap.put("timesuuid", "java.util.Date");
            typeNameMap.put("varchar", "java.lang.String");
            typeNameMap.put("varint", "java.math.BigInteger");
        }

        private final String name;
        private final String tableName;
        private final String typeName;
        private final String keyspace;

        public ColumnMetaData(String name, String tableName, String keyspace, String typeName) {
            this.name = name;
            this.tableName = tableName;
            this.typeName = typeName;
            this.keyspace = keyspace;
        }

        int getJavaType() {
            String lower = typeName.toLowerCase();
            if (javaTypeMap.containsKey(lower)) return javaTypeMap.get(lower);
            throw new IllegalArgumentException("Type name is not known: " + lower);
        }

        String getClassName() {
            String lower = typeName.toLowerCase();
            if (typeNameMap.containsKey(lower)) return typeNameMap.get(lower);
            throw new IllegalArgumentException("Type name is not known: " + typeName);
        }
    }
}
