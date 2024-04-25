package com.wisecoders.dbschema.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.wisecoders.dbschema.cassandra.types.ArrayResultSet;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.wisecoders.dbschema.cassandra.JdbcDriver.LOGGER;
/**
 * A Cassandra database is used as a catalogs by this driver. Schemas aren't used. A Cassandra collection is equivalent to a tables, in that each collection is a table.
 *
 * Licensed under <a href="https://creativecommons.org/licenses/by-nd/4.0/">CC BY-ND 4.0 DEED</a>, copyright <a href="https://wisecoders.com">Wise Coders GmbH</a>, used by <a href="https://dbschema.com">DbSchema Database Designer</a>.
 * Code modifications allowed only as pull requests to the <a href="https://github.com/wise-coders/cassandra-jdbc-driver">public GIT repository</a>.
 */

public class CassandraMetaData implements DatabaseMetaData {

    private final CassandraConnection connection;
    private JdbcDriver driver;

    CassandraMetaData(CassandraConnection connection, JdbcDriver driver) {
        this.connection = connection;
        this.driver = driver;
    }

    @Override
    public ResultSet getSchemas() {
        return new ArrayResultSet("TABLE_SCHEMA", "TABLE_CATALOG");
    }

    /**
     * @see java.sql.DatabaseMetaData#getCatalogs()
     */
    @Override
    public ResultSet getCatalogs()
    {
        final List<String> loadedKeyspaces = new ArrayList<>();
        final ArrayResultSet retVal = new ArrayResultSet();
        retVal.setColumnNames(new String[]{"TABLE_CAT"});
        for ( CqlIdentifier identifier : connection.getSession().getMetadata().getKeyspaces().keySet() ){
            retVal.addRow(new String[] { identifier.toString() });
            loadedKeyspaces.add( identifier.toString());
        }
        try ( ResultSet rs = connection.prepareStatement("SELECT keyspace_name FROM system_schema.keyspaces").executeQuery() ){
            while( rs.next() ) {
                if ( !loadedKeyspaces.contains( rs.getString(1 ))) {
                    retVal.addRow(new String[]{rs.getString(1)});
                    loadedKeyspaces.add( rs.getString(1 ));
                }
            }
        } catch ( SQLException ex ){ LOGGER.warning( "Error loading keyspaces using query 'SELECT keyspace_name FROM system_schema.keyspaces': " + ex );}
        return retVal;
    }

    public ResultSet getTables(String catalogName, String schemaPattern, String tableNamePattern, String[] types) {
        ArrayResultSet resultSet = new ArrayResultSet();
        resultSet.setColumnNames(new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME",
                "REF_GENERATION"});

        this.connection.getSession().getMetadata().getKeyspace(catalogName).ifPresent( metadata -> {
            for (Map.Entry<CqlIdentifier, TableMetadata> entry: metadata.getTables().entrySet()){
                CqlIdentifier cqlIdentifier = entry.getKey();

                String[] data = new String[10];
                data[0] = catalogName; // TABLE_CAT
                data[1] = ""; // TABLE_SCHEM
                data[2] = cqlIdentifier.toString(); // TABLE_NAME
                data[3] = "TABLE"; // TABLE_TYPE
                data[4] = null; // REMARKS
                data[5] = ""; // TYPE_CAT
                data[6] = ""; // TYPE_SCHEM
                data[7] = ""; // TYPE_NAME
                data[8] = ""; // SELF_REFERENCING_COL_NAME
                data[9] = ""; // REF_GENERATION

                resultSet.addRow( data );
            }
        });

        return resultSet;
    }


    /**
     * @see java.sql.DatabaseMetaData#getColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getColumns(String catalogName, String schemaName, String tableNamePattern, String columnNamePattern) {

        final ArrayResultSet result = new ArrayResultSet();
        result.setColumnNames(new String[] { "TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME",
                "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
                "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
                "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "OPTIONS" });

        connection.getSession().getMetadata().getKeyspace( catalogName ).ifPresent( metadata -> {
            if ( tableNamePattern == null ){
                for ( TableMetadata tableMetadata : metadata.getTables().values() ){
                    for ( ColumnMetadata field : tableMetadata.getColumns().values() ){
                        if ( columnNamePattern == null || columnNamePattern.equals( String.valueOf( field ))){
                            exportColumnsRecursive( tableMetadata, result, field);
                        }
                    }
                }
            } else {
                metadata.getTable( tableNamePattern ).ifPresent( tableMetadata -> {
                    for (ColumnMetadata field : tableMetadata.getColumns().values() ) {
                        if (columnNamePattern == null || columnNamePattern.equals(String.valueOf( field ))) {
                            exportColumnsRecursive(tableMetadata, result, field);
                        }
                    }
                } );
            }
        } );
        return result;
    }

    private void exportColumnsRecursive( TableMetadata tableMetadata, ArrayResultSet result, ColumnMetadata columnMetadata) {
        StringBuilder sb = new StringBuilder();
        /*
        TableOptionsMetadata options = tableMetadata.getOptions();
        if( options != null ){
            sb.append("bloom_filter_fp_chance = " ).append(options.getBloomFilterFalsePositiveChance() ).
                    append("\n AND caching = '").append( options.getCaching()).append("'").
                    append("\n  AND comment = '").append( options.getComment()).append("'").
                    append("\n  AND compaction = ").append( options.getCompaction()).
                    append("\n  AND compression = ").append( options.getCompression()).
                    append("\n  AND dclocal_read_repair_chance = " ).append(options.getLocalReadRepairChance() ).
                    append("\n  AND default_time_to_live = " ).append(options.getDefaultTimeToLive() ).
                    append("\n  AND gc_grace_seconds = " ).append(options.getGcGraceInSeconds() ).
                    append("\n  AND max_index_interval = " ).append(options.getMaxIndexInterval() ).
                    append("\n  AND memtable_flush_period_in_ms = " ).append(options.getMemtableFlushPeriodInMs() ).
                    append("\n  AND min_index_interval = " ).append(options.getMinIndexInterval() ).
                    append("\n  AND read_repair_chance = " ).append(options.getReadRepairChance() ).
                    append("\n  AND speculative_retry = '").append(options.getSpeculativeRetry() ).append("'");
        }*/

        result.addRow(new String[] {
                String.valueOf(tableMetadata.getKeyspace()), // "TABLE_CAT",
                null, // "TABLE_SCHEMA",
                String.valueOf( tableMetadata ), // "TABLE_NAME", (i.e. Cassandra Collection Name)
                String.valueOf( columnMetadata ), // "COLUMN_NAME",
                "" + columnMetadata.getType(), // "DATA_TYPE",
                "" + columnMetadata.getType(), // "TYPE_NAME", -- I LET THIS INTENTIONALLY TO USE .toString() BECAUSE OF USER DEFINED TYPES.
                "800", // "COLUMN_SIZE",
                "0", // "BUFFER_LENGTH", (not used)
                "0", // "DECIMAL_DIGITS",
                "10", // "NUM_PREC_RADIX",
                "0", // "NULLABLE", // I RETREIVE HERE IF IS FROZEN ( MANDATORY ) OR NOT ( NULLABLE )
                "", // "REMARKS",
                "", // "COLUMN_DEF",
                "0", // "SQL_DATA_TYPE", (not used)
                "0", // "SQL_DATETIME_SUB", (not used)
                "800", // "CHAR_OCTET_LENGTH",
                "1", // "ORDINAL_POSITION",
                "NO", // "IS_NULLABLE",
                null, // "SCOPE_CATLOG", (not a REF type)
                null, // "SCOPE_SCHEMA", (not a REF type)
                null, // "SCOPE_TABLE", (not a REF type)
                null, // "SOURCE_DATA_TYPE", (not a DISTINCT or REF type)
                "NO", // "IS_AUTOINCREMENT" (can be auto-generated, but can also be specified)
                sb.toString() // TABLE_OPTIONS
        });
    }


    /**
     * @see java.sql.DatabaseMetaData#getPrimaryKeys(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getPrimaryKeys(String catalogName, String schemaName, String tableNamePattern) {

        final ArrayResultSet result = new ArrayResultSet();
        result.setColumnNames(new String[] { "TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME" });

        connection.getSession().getMetadata().getKeyspace( catalogName ).ifPresent( keyspaceMetadata -> {
            keyspaceMetadata.getTable( tableNamePattern ).ifPresent( tableMetadata -> {
                int seq = 0;
                for ( ColumnMetadata columnMetadata : tableMetadata.getPrimaryKey() ){
                    result.addRow(new String[]{
                            String.valueOf( keyspaceMetadata), // "TABLE_CAT",
                            null, // "TABLE_SCHEMA",
                            String.valueOf( tableMetadata), // "TABLE_NAME", (i.e. Cassandra Collection Name)
                            String.valueOf(columnMetadata), // "COLUMN_NAME",
                            "" + seq++, // "ORDINAL_POSITION"
                            "PRIMARY KEY" // "PK_NAME"
                    });
                }
            });
        });
        return result;
    }

    /**
     * @see java.sql.DatabaseMetaData#getIndexInfo(java.lang.String, java.lang.String, java.lang.String,
     *      boolean, boolean)
     */
    public ResultSet getIndexInfo(String catalogName, String schemaName, String tableNamePattern, boolean unique,  boolean approximate) {
        final ArrayResultSet result = new ArrayResultSet();
        result.setColumnNames(new String[]{"TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE",
                "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                "CARDINALITY", "PAGES", "FILTER_CONDITION"});

        connection.getSession().getMetadata().getKeyspace( catalogName ).ifPresent( keyspaceMetadata -> {
            keyspaceMetadata.getTable( tableNamePattern ).ifPresent( tableMetadata -> {
                int seq = 0;
                for ( Map.Entry<ColumnMetadata, ClusteringOrder> entry : tableMetadata.getClusteringColumns().entrySet() ) {
                    result.addRow(new String[]{
                            String.valueOf(keyspaceMetadata), // "TABLE_CAT",
                            null, // "TABLE_SCHEMA",
                            String.valueOf( tableMetadata.getName() ), // "TABLE_NAME", (i.e. Cassandra Collection Name)
                            "FALSE", // "NON-UNIQUE",
                            String.valueOf( entry.getKey().getName()), // "INDEX QUALIFIER",
                            "CLUSTER KEY", // "INDEX_NAME",
                            "0", // "TYPE",
                            "" + seq++ , // "ORDINAL_POSITION"
                            String.valueOf( entry.getKey().getName() ), // "COLUMN_NAME",
                            entry.getValue() == ClusteringOrder.ASC ? "A" : "D", // "ASC_OR_DESC",
                            "0", // "CARDINALITY",
                            "0", // "PAGES",
                            "" // "FILTER_CONDITION",
                    });
                }
            });
        });
        return result;
    }

    private List<String> listColumnNames(String query ){
        final List<String> ret = new ArrayList<String>();
        if ( query != null ){
            int idx = query.indexOf("(");
            if ( idx > 0 ) query = query.substring( idx+1 );
            idx = query.lastIndexOf(")");
            if ( idx > 0 ) query = query.substring( 0, idx );
            for ( String term : query.split(",")){
                term = term.trim();
                if ( term.length() > 0 ){
                    ret.add( term );
                }
            }
        }
        return ret;
    }


    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return null;
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

    public String getURL() {
        return null;
    }

    public String getUserName() {
        return null;
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
        com.datastax.oss.driver.api.core.cql.ResultSet result = connection.getSession().execute("select release_version from system.local");
        return result.one().getString(0);
    }

    public String getDriverName() {
        return "Cassandra JDBC Driver";
    }

    public String getDriverVersion() {
        return driver.getVersion();
    }

    public int getDriverMajorVersion() {
        return driver.getMajorVersion();
    }

    public int getDriverMinorVersion() {
        return driver.getMinorVersion();
    }

    public boolean usesLocalFiles() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsMixedCaseIdentifiers() {
        return false;
    }

    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    public boolean storesLowerCaseIdentifiers() {
        return true;
    }

    public boolean storesMixedCaseIdentifiers() {
        return false;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    public boolean storesUpperCaseQuotedIdentifiers() {
        return true;
    }

    public boolean storesLowerCaseQuotedIdentifiers() {
        return true;
    }

    public boolean storesMixedCaseQuotedIdentifiers() {
        return true;
    }

    public String getIdentifierQuoteString() {
        return "\"";
    }

    public String getSQLKeywords() {
        return null;
    }

    public String getNumericFunctions() {
        return null;
    }

    public String getStringFunctions() {
        return null;
    }

    public String getSystemFunctions() {
        return null;
    }

    public String getTimeDateFunctions() {
        return null;
    }

    public String getSearchStringEscape() {
        return null;
    }

    public String getExtraNameCharacters() {
        return "";
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
                                   String procedureNamePattern) {
        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalogName, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) {
        return null;
    }

    @Override
    public ResultSet getTableTypes() {
        return null;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalogName, String schemaName,
                                         String table, String columnNamePattern) {
        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalogName, String schemaPattern, String tableNamePattern) {
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalogName, String schemaName, String table, int scope,
                                          boolean nullable) {
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalogName, String schemaName, String table) {
        return null;
    }

    @Override
    public ResultSet getExportedKeys(String catalogName, String schemaName, String tableNamePattern) {
        return null;
    }

    @Override
    public ResultSet getImportedKeys(String catalogName, String schemaName, String tableNamePattern) {
        return null;
    }


    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) {
        return null;
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
    public ResultSet getUDTs(String catalogName, String schemaPattern, String typeNamePattern, int[] types) {
        return null;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
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
    public ResultSet getSuperTypes(String catalogName, String schemaPattern, String typeNamePattern) {
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalogName, String schemaPattern, String tableNamePattern) {
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalogName, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) {
        return null;
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
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 2;
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
    public ResultSet getSchemas(String catalogName, String schemaPattern) {
        return null;
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
    public ResultSet getFunctions(String catalogName, String schemaPattern, String functionNamePattern) {
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalogName, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) {
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalogName, String schemaPattern, String tableNamePattern, String columnNamePattern) {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
