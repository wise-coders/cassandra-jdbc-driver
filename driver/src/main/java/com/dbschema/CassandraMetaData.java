package com.dbschema;

import java.sql.*;

/**
 * Cassandra databases are equivalent to catalogs for this driver. Schemas aren't used. Cassandra collections are
 * equivalent to tables, in that each collection is a table.
 */
public class CassandraMetaData implements DatabaseMetaData {

    private static final int TYPE_MAP = 4999544;
    private static final int TYPE_LIST = 4999545;

    private final CassandraConnection con;

    CassandraMetaData(CassandraConnection con) {
        this.con = con;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public ResultSet getTables(String catalogName, String schemaPattern,
                               String tableNamePattern, String[] types) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public ResultSet getColumns(String catalogName, String schemaName,
                                String tableNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public ResultSet getPrimaryKeys(String catalogName, String schemaName, String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public ResultSet getIndexInfo(String catalogName, String schemaName, String tableNamePattern, boolean unique,
                                  boolean approximate) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public ResultSet getTypeInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean allProceduresAreCallable() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean allTablesAreSelectable() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getURL() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getUserName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean isReadOnly() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean nullsAreSortedHigh() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean nullsAreSortedLow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getDatabaseProductName() {
        return "Cassandra";
    }

    public String getDatabaseProductVersion() {
        return "1.0";
    }

    public String getDriverName() {
        return "Cassandra JDBC Driver";
    }

    public String getDriverVersion() {
        return "1.2.3-SNAPSHOT";
    }

    public int getDriverMajorVersion() {
        return 1;
    }

    public int getDriverMinorVersion() {
        return 23;
    }

    public boolean usesLocalFiles() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getIdentifierQuoteString() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getSQLKeywords() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getNumericFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getStringFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getSystemFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getTimeDateFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getSearchStringEscape() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getExtraNameCharacters() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsColumnAliasing() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsConvert() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsOrderByUnrelated() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsGroupBy() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsGroupByUnrelated() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsNonNullableColumns() {
        return true;
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsCoreSQLGrammar() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsANSI92FullSQL() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsOuterJoins() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsFullOuterJoins() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getSchemaTerm() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getProcedureTerm() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getCatalogTerm() {
        return "keyspace";
    }

    public boolean isCatalogAtStart() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getCatalogSeparator() {
        return ".";
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsCatalogsInDataManipulation() {
        return true;
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsPositionedDelete() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSelectForUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsStoredProcedures() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsUnion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsUnionAll() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxBinaryLiteralLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxCharLiteralLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxColumnNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxColumnsInGroupBy() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxColumnsInIndex() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxColumnsInOrderBy() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxColumnsInSelect() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxColumnsInTable() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxConnections() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxCursorNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxIndexLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxSchemaNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxProcedureNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxCatalogNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxRowSize() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxStatementLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxStatements() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxTableNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxTablesInSelect() {
        return 1;
    }

    public int getMaxUserNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    /**
     * Cassandra doesn't support transactions, but document updates are atomic.
     */
    public boolean supportsTransactions() {
        return false;
    }

    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public ResultSet getProcedures(String catalogName, String schemaPattern,
                                   String procedureNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getProcedureColumns(String catalogName, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public ResultSet getColumnPrivileges(String catalogName, String schemaName, String table, String columnNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getTablePrivileges(String catalogName, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalogName, String schemaName, String table, int scope,
                                          boolean nullable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getVersionColumns(String catalogName, String schemaName, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getExportedKeys(String catalogName, String schemaName, String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getImportedKeys(String catalogName, String schemaName, String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean supportsBatchUpdates() {
        return true;
    }

    @Override
    public ResultSet getUDTs(String catalogName, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Connection getConnection() {
        return con;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getSuperTypes(String catalogName, String schemaPattern, String typeNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getSuperTables(String catalogName, String schemaPattern, String tableNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getAttributes(String catalogName, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getDatabaseMajorVersion() {
        return Integer.parseInt((getDatabaseProductVersion().split("\\."))[0]);
    }

    @Override
    public int getDatabaseMinorVersion() {
        return Integer.parseInt((getDatabaseProductVersion().split("\\."))[1]);
    }

    @Override
    public int getJDBCMajorVersion() {
        return 1;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 0;
    }

    @Override
    public int getSQLStateType() {
        return DatabaseMetaData.sqlStateXOpen;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getSchemas(String catalogName, String schemaPattern) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getFunctions(String catalogName, String schemaPattern, String functionNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getFunctionColumns(String catalogName, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getPseudoColumns(String catalogName, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public static int getJavaTypeByName(String typeName) {
        if ("ascii".equalsIgnoreCase(typeName)) return Types.VARCHAR;
        if ("bigint".equalsIgnoreCase(typeName)) return Types.BIGINT;
        if ("blob".equalsIgnoreCase(typeName)) return Types.BLOB;
        if ("boolean".equalsIgnoreCase(typeName)) return Types.BOOLEAN;
        if ("counter".equalsIgnoreCase(typeName)) return Types.NUMERIC;
        if ("decimal".equalsIgnoreCase(typeName)) return Types.DECIMAL;
        if ("double".equalsIgnoreCase(typeName)) return Types.DOUBLE;
        if ("float".equalsIgnoreCase(typeName)) return Types.FLOAT;
        if ("inet".equalsIgnoreCase(typeName)) return Types.VARCHAR;
        if ("int".equalsIgnoreCase(typeName)) return Types.INTEGER;
        if ("list".equalsIgnoreCase(typeName)) return TYPE_LIST;
        if ("map".equalsIgnoreCase(typeName)) return TYPE_MAP;
        if ("set".equalsIgnoreCase(typeName)) return Types.STRUCT;
        if ("text".equalsIgnoreCase(typeName)) return Types.VARCHAR;
        if ("timestamp".equalsIgnoreCase(typeName)) return Types.TIMESTAMP;
        if ("uuid".equalsIgnoreCase(typeName)) return Types.ROWID;
        if ("timesuuid".equalsIgnoreCase(typeName)) return Types.ROWID;
        if ("varchar".equalsIgnoreCase(typeName)) return Types.VARCHAR;
        if ("varint".equalsIgnoreCase(typeName)) return Types.INTEGER;
        throw new IllegalArgumentException("Type name is not known: " + typeName);
    }
}
