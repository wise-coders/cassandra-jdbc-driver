package com.dbschema;

import com.datastax.driver.core.*;
import com.dbschema.resultSet.ArrayResultSet;

import java.sql.Connection;
import java.sql.*;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Cassandra databases are equivalent to catalogs for this driver. Schemas aren't used. Cassandra collections are
 * equivalent to tables, in that each collection is a table.
 */
public class CassandraMetaData implements DatabaseMetaData {

    public static final int TYPE_MAP = 4999544;
    public static final int TYPE_LIST = 4999545;

    private final CassandraConnection con;

    private final static ArrayResultSet EMPTY_RESULT_SET = new ArrayResultSet();
    public final static String OBJECT_ID_TYPE_NAME = "OBJECT_ID";
    public final static String DOCUMENT_TYPE_NAME = "DOCUMENT";




    public CassandraMetaData(CassandraConnection con) {
        this.con = con;
    }

    /**
     * @see java.sql.DatabaseMetaData#getSchemas()
     */
    @Override
    public ResultSet getSchemas() throws SQLException
    {
        ArrayResultSet retVal = new ArrayResultSet();
        retVal.setColumnNames(new String[] { "TABLE_SCHEMA", "TABLE_CATALOG" });
        return retVal;
    }

    /**
     * @see java.sql.DatabaseMetaData#getCatalogs()
     */
    @Override
    public ResultSet getCatalogs() throws SQLException
    {
        ArrayResultSet retVal = new ArrayResultSet();
        retVal.setColumnNames(new String[]{"TABLE_CAT"});
        for ( KeyspaceMetadata kmd : con.session.getCluster().getMetadata().getKeyspaces() ){
            retVal.addRow(new String[] { kmd.getName() });
        }
        return retVal;
        //for (Row row : con.getSession().execute("describe keyspace").all()) {
    }



    /**
     * @see java.sql.DatabaseMetaData#getTables(java.lang.String, java.lang.String, java.lang.String,
     *      java.lang.String[])
     */
    public ResultSet getTables(String catalogName, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException
    {
        ArrayResultSet resultSet = new ArrayResultSet();
        resultSet.setColumnNames(new String[]{"TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME",
                "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME",
                "REF_GENERATION"});
        final Metadata metadata = con.session.getCluster().getMetadata();
        if ( catalogName != null && catalogName.trim().isEmpty() ) {
            KeyspaceMetadata keyspace = metadata.getKeyspace(catalogName);
            System.out.println("Loading tables for Catalog " + catalogName);
            if ( keyspace == null ) metadata.getKeyspace("\"" + catalogName + "\"" );
            if ( keyspace != null) {
                for (TableMetadata tableMetadata : keyspace.getTables()) {
                    resultSet.addRow(createTableRow(catalogName, tableMetadata.getName(), tableMetadata.getOptions().getComment(), tableMetadata.getOptions()));
                }
                return resultSet;
            }
            System.out.println("Could not find any keyspace '" + catalogName + "'. Will list all keyspaces.");
        }
        for ( KeyspaceMetadata keyspaceMetadata : metadata.getKeyspaces() ) {
            for (TableMetadata tableMetadata : keyspaceMetadata.getTables()) {
                resultSet.addRow(createTableRow(catalogName, tableMetadata.getName(), tableMetadata.getOptions().getComment(), tableMetadata.getOptions()));
            }
        }
        return resultSet;
    }

    private String[] createTableRow( String catalogName, String tableName, String comment, TableOptionsMetadata options ){
        String[] data = new String[10];
        data[0] = catalogName; // TABLE_CAT
        data[1] = ""; // TABLE_SCHEMA
        data[2] = tableName; // TABLE_NAME
        data[3] = "TABLE"; // TABLE_TYPE
        data[4] = comment; // REMARKS
        data[5] = ""; // TYPE_CAT
        data[6] = ""; // TYPE_SCHEM
        data[7] = ""; // TYPE_NAME
        data[8] = ""; // SELF_REFERENCING_COL_NAME
        data[9] = ""; // REF_GENERATION
        return data;
    }


    /**
     * @see java.sql.DatabaseMetaData#getColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getColumns(String catalogName, String schemaName, String tableNamePattern, String columnNamePattern) throws SQLException
    {

        final ArrayResultSet result = new ArrayResultSet();
        result.setColumnNames(new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
                "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATLOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
                "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "OPTIONS" });

        final KeyspaceMetadata metadata = con.session.getCluster().getMetadata().getKeyspace( catalogName );
        if ( metadata != null ){
            if ( tableNamePattern == null ){
                for ( TableMetadata tableMetadata : metadata.getTables()){
                    for ( ColumnMetadata field : tableMetadata.getColumns() ){
                        if ( columnNamePattern == null || columnNamePattern.equals( field.getName())){
                            exportColumnsRecursive( tableMetadata, result, field);
                        }
                    }
                }
            } else {
                final TableMetadata tableMetadata = metadata.getTable( tableNamePattern );
                if ( tableMetadata != null ) {
                    for (ColumnMetadata field : tableMetadata.getColumns()) {
                        if (columnNamePattern == null || columnNamePattern.equals(field.getName())) {
                            exportColumnsRecursive(tableMetadata, result, field);
                        }
                    }
                }
            }
        }
        return result;
    }

    private void exportColumnsRecursive( TableMetadata tableMetadata, ArrayResultSet result, ColumnMetadata columnMetadata) {
        StringBuilder sb = new StringBuilder();
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
        }

        result.addRow(new String[] {
                tableMetadata.getKeyspace().getName(), // "TABLE_CAT",
                null, // "TABLE_SCHEMA",
                tableMetadata.getName(), // "TABLE_NAME", (i.e. Cassandra Collection Name)
                columnMetadata.getName(), // "COLUMN_NAME",
                "" + CassandraMetaData.getJavaTypeByName( "" + columnMetadata.getType().getName() ), // "DATA_TYPE",
                "" + columnMetadata.getType().getName(), // "TYPE_NAME",
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
    public ResultSet getPrimaryKeys(String catalogName, String schemaName, String tableNamePattern) throws SQLException
    {
        /*
        * 	<LI><B>TABLE_CAT</B> String => table catalog (may be <code>null</code>)
       *	<LI><B>TABLE_SCHEM</B> String => table schema (may be <code>null</code>)
       *	<LI><B>TABLE_NAME</B> String => table name
       *	<LI><B>COLUMN_NAME</B> String => column name
       *	<LI><B>KEY_SEQ</B> short => sequence number within primary key( a value
       *  of 1 represents the first column of the primary key, a value of 2 would
       *  represent the second column within the primary key).
       *	<LI><B>PK_NAME</B> Stri
        *
        */

        final ArrayResultSet result = new ArrayResultSet();
        result.setColumnNames(new String[] { "TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME" });

        final KeyspaceMetadata metadata = con.session.getCluster().getMetadata().getKeyspace( catalogName );
        if ( metadata != null ){
            final TableMetadata tableMetadata = metadata.getTable( tableNamePattern );
            if ( tableMetadata != null ){
                int seq = 0;
                for ( ColumnMetadata columnMetadata : tableMetadata.getPartitionKey() ){
                    result.addRow(new String[]{
                            metadata.getName(), // "TABLE_CAT",
                            null, // "TABLE_SCHEMA",
                            tableMetadata.getName(), // "TABLE_NAME", (i.e. Cassandra Collection Name)
                            columnMetadata.getName(), // "COLUMN_NAME",
                            "" + seq++, // "ORDINAL_POSITION"
                            "PARTITION KEY" // "PK_NAME"
                    });
                }
            }
        }
        return result;
    }

    /**
     * @see java.sql.DatabaseMetaData#getIndexInfo(java.lang.String, java.lang.String, java.lang.String,
     *      boolean, boolean)
     */
    public ResultSet getIndexInfo(String catalogName, String schemaName, String tableNamePattern, boolean unique,
                                  boolean approximate) throws SQLException
    {
        /*
        *      *  <OL>
            *	<LI><B>TABLE_CAT</B> String => table catalog (may be <code>null</code>)
            *	<LI><B>TABLE_SCHEMA</B> String => table schema (may be <code>null</code>)
            *	<LI><B>TABLE_NAME</B> String => table name
            *	<LI><B>NON_UNIQUE</B> boolean => Can index values be non-unique.
            *      false when TYPE is tableIndexStatistic
            *	<LI><B>INDEX_QUALIFIER</B> String => index catalog (may be <code>null</code>);
            *      <code>null</code> when TYPE is tableIndexStatistic
            *	<LI><B>INDEX_NAME</B> String => index name; <code>null</code> when TYPE is
            *      tableIndexStatistic
            *	<LI><B>TYPE</B> short => index type:
            *      <UL>
            *      <LI> tableIndexStatistic - this identifies table statistics that are
            *           returned in conjuction with a table's index descriptions
            *      <LI> tableIndexClustered - this is a clustered index
            *      <LI> tableIndexHashed - this is a hashed index
            *      <LI> tableIndexOther - this is some other style of index
            *      </UL>
            *	<LI><B>ORDINAL_POSITION</B> short => column sequence number
            *      within index; zero when TYPE is tableIndexStatistic
            *	<LI><B>COLUMN_NAME</B> String => column name; <code>null</code> when TYPE is
            *      tableIndexStatistic
            *	<LI><B>ASC_OR_DESC</B> String => column sort sequence, "A" => ascending,
            *      "D" => descending, may be <code>null</code> if sort sequence is not supported;
            *      <code>null</code> when TYPE is tableIndexStatistic
            *	<LI><B>CARDINALITY</B> int => When TYPE is tableIndexStatistic, then
            *      this is the number of rows in the table; otherwise, it is the
            *      number of unique values in the index.
            *	<LI><B>PAGES</B> int => When TYPE is  tableIndexStatisic then
            *      this is the number of pages used for the table, otherwise it
            *      is the number of pages used for the current index.
            *	<LI><B>FILTER_CONDITION</B> String => Filter condition, if any.
            *      (may be <code>null</code>)
            *  </OL>
        */
        final ArrayResultSet result = new ArrayResultSet();
        result.setColumnNames(new String[]{"TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE",
                "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                "CARDINALITY", "PAGES", "FILTER_CONDITION"});

        final KeyspaceMetadata metadata = con.session.getCluster().getMetadata().getKeyspace( catalogName );
        if ( metadata != null ){
            final TableMetadata tableMetadata = metadata.getTable( tableNamePattern );
            if ( tableMetadata != null ){

                final Iterator<ColumnMetadata> itrColMeta = tableMetadata.getClusteringColumns().iterator();
                final Iterator<ClusteringOrder> itrColOrder = tableMetadata.getClusteringOrder().iterator();
                int seq = 0;
                for ( ; itrColMeta.hasNext() && itrColOrder.hasNext(); ){
                    result.addRow(new String[]{
                            metadata.getName(), // "TABLE_CAT",
                            null, // "TABLE_SCHEMA",
                            tableMetadata.getName(), // "TABLE_NAME", (i.e. Cassandra Collection Name)
                            "FALSE", // "NON-UNIQUE",
                            metadata.getName(), // "INDEX QUALIFIER",
                            "CLUSTER KEY", // "INDEX_NAME",
                            "0", // "TYPE",
                            "" + seq++ , // "ORDINAL_POSITION"
                            itrColMeta.next().getName(), // "COLUMN_NAME",
                            itrColOrder.next()== ClusteringOrder.ASC ? "A" : "D", // "ASC_OR_DESC",
                            "0", // "CARDINALITY",
                            "0", // "PAGES",
                            "" // "FILTER_CONDITION",
                    });
                }

                for ( IndexMetadata indexMetadata : tableMetadata.getIndexes() ){
                    seq = 0;
                    for ( String colName : listColumnNames(indexMetadata.asCQLQuery())){
                        result.addRow(new String[] {
                                metadata.getName(), // "TABLE_CAT",
                                null, // "TABLE_SCHEMA",
                                tableMetadata.getName(), // "TABLE_NAME", (i.e. Cassandra Collection Name)
                                "TRUE", // "NON-UNIQUE",
                                metadata.getName(), // "INDEX QUALIFIER",
                                indexMetadata.getName(), // "INDEX_NAME",
                                "0", // "TYPE",
                                "" + seq++ , // "ORDINAL_POSITION"
                                colName, // "COLUMN_NAME",
                                "A", // "ASC_OR_DESC",
                                "0", // "CARDINALITY",
                                "0", // "PAGES",
                                "" // "FILTER_CONDITION",
                        });
                    }
                }
            }
        }
        return result;
    }

    private List<String> listColumnNames( String query ){
        final List<String> ret = new ArrayList<String>();
        if ( query != null ){
            int idx = query.indexOf("(");
            if ( idx > 0 ) query = query.substring( idx+1 );
            query = query.replaceAll("\\(" , "," );
            query = query.replaceAll("\\)" , "," );
            for ( String term : query.split(",")){
                term = term.trim();
                if ( term.length() > 0 ){
                    ret.add( term );
                }
            }
        }
        return ret;
    }

    /**
     * @see java.sql.DatabaseMetaData#getTypeInfo()
     */
    public ResultSet getTypeInfo() throws SQLException
    {
        /*
            * <P>Each type description has the following columns:
            *  <OL>
            *	<LI><B>TYPE_NAME</B> String => Type name
            *	<LI><B>DATA_TYPE</B> int => SQL data type from java.sql.Types
            *	<LI><B>PRECISION</B> int => maximum precision
            *	<LI><B>LITERAL_PREFIX</B> String => prefix used to quote a literal
            *      (may be <code>null</code>)
            *	<LI><B>LITERAL_SUFFIX</B> String => suffix used to quote a literal
            (may be <code>null</code>)
            *	<LI><B>CREATE_PARAMS</B> String => parameters used in creating
            *      the type (may be <code>null</code>)
            *	<LI><B>NULLABLE</B> short => can you use NULL for this type.
            *      <UL>
            *      <LI> typeNoNulls - does not allow NULL values
            *      <LI> typeNullable - allows NULL values
            *      <LI> typeNullableUnknown - nullability unknown
            *      </UL>
            *	<LI><B>CASE_SENSITIVE</B> boolean=> is it case sensitive.
            *	<LI><B>SEARCHABLE</B> short => can you use "WHERE" based on this type:
            *      <UL>
            *      <LI> typePredNone - No support
            *      <LI> typePredChar - Only supported with WHERE .. LIKE
            *      <LI> typePredBasic - Supported except for WHERE .. LIKE
            *      <LI> typeSearchable - Supported for all WHERE ..
            *      </UL>
            *	<LI><B>UNSIGNED_ATTRIBUTE</B> boolean => is it unsigned.
            *	<LI><B>FIXED_PREC_SCALE</B> boolean => can it be a money value.
            *	<LI><B>AUTO_INCREMENT</B> boolean => can it be used for an
            *      auto-increment value.
            *	<LI><B>LOCAL_TYPE_NAME</B> String => localized version of type name
            *      (may be <code>null</code>)
            *	<LI><B>MINIMUM_SCALE</B> short => minimum scale supported
            *	<LI><B>MAXIMUM_SCALE</B> short => maximum scale supported
            *	<LI><B>SQL_DATA_TYPE</B> int => unused
            *	<LI><B>SQL_DATETIME_SUB</B> int => unused
            *	<LI><B>NUM_PREC_RADIX</B> int => usually 2 or 10
            *  </OL>
        */
        ArrayResultSet retVal = new ArrayResultSet();
        retVal.setColumnNames(new String[] { "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX",
                "LITERAL_SUFFIX", "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE",
                "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE",
                "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX" });

        retVal.addRow(new String[] { OBJECT_ID_TYPE_NAME, // "TYPE_NAME",
                "" + Types.VARCHAR, // "DATA_TYPE",
                "800", // "PRECISION",
                "'", // "LITERAL_PREFIX",
                "'", // "LITERAL_SUFFIX",
                null, // "CREATE_PARAMS",
                "" + typeNullable, // "NULLABLE",
                "true", // "CASE_SENSITIVE",
                "" + typeSearchable, // "SEARCHABLE",
                "false", // "UNSIGNED_ATTRIBUTE",
                "false", // "FIXED_PREC_SCALE",
                "false", // "AUTO_INCREMENT",
                OBJECT_ID_TYPE_NAME, // "LOCAL_TYPE_NAME",
                "0", // "MINIMUM_SCALE",
                "0", // "MAXIMUM_SCALE",
                null, // "SQL_DATA_TYPE", (not used)
                null, // "SQL_DATETIME_SUB", (not used)
                "10", // "NUM_PREC_RADIX" (javadoc says usually 2 or 10)
        });

        retVal.addRow(new String[] { DOCUMENT_TYPE_NAME, // "TYPE_NAME",
                "" + Types.CLOB, // "DATA_TYPE",
                "16777216", // "PRECISION",
                "'", // "LITERAL_PREFIX",
                "'", // "LITERAL_SUFFIX",
                null, // "CREATE_PARAMS",
                "" + typeNullable, // "NULLABLE",
                "true", // "CASE_SENSITIVE",
                "" + typeSearchable, // "SEARCHABLE",
                "false", // "UNSIGNED_ATTRIBUTE",
                "false", // "FIXED_PREC_SCALE",
                "false", // "AUTO_INCREMENT",
                DOCUMENT_TYPE_NAME, // "LOCAL_TYPE_NAME",
                "0", // "MINIMUM_SCALE",
                "0", // "MAXIMUM_SCALE",
                null, // "SQL_DATA_TYPE", (not used)
                null, // "SQL_DATETIME_SUB", (not used)
                "10", // "NUM_PREC_RADIX" (javadoc says usually 2 or 10)
        });
        return retVal;
    }








    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    public boolean allTablesAreSelectable() throws SQLException {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#getURL()
     */
    public String getURL() throws SQLException {
        return "";
    }

    public String getUserName() throws SQLException {
        return null;
    }

    public boolean isReadOnly() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedHigh() throws SQLException
    {
        return false;
    }

    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#getDatabaseProductName()
     */
    public String getDatabaseProductName() throws SQLException {
        return "Cassandra";
    }

    /**
     * @see java.sql.DatabaseMetaData#getDatabaseProductVersion()
     */
    public String getDatabaseProductVersion() throws SQLException {
        return "1.0";
    }

    /**
     * @see java.sql.DatabaseMetaData#getDriverName()
     */
    public String getDriverName() throws SQLException
    {
        return "Cassandra JDBC Driver";
    }

    /**
     * @see java.sql.DatabaseMetaData#getDriverVersion()
     */
    public String getDriverVersion() throws SQLException
    {
        return "1.0";
    }

    /**
     * @see java.sql.DatabaseMetaData#getDriverMajorVersion()
     */
    public int getDriverMajorVersion()
    {
        return 1;
    }

    /**
     * @see java.sql.DatabaseMetaData#getDriverMinorVersion()
     */
    public int getDriverMinorVersion()
    {
        return 0;
    }

    public boolean usesLocalFiles() throws SQLException
    {

        return false;
    }

    public boolean usesLocalFilePerTable() throws SQLException
    {
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException
    {
        return false;
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException
    {
        return false;
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException
    {
        return false;
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException
    {
        return false;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
    {
        return false;
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
    {
        return false;
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
    {
        return false;
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
    {

        return false;
    }

    public String getIdentifierQuoteString() throws SQLException
    {

        return null;
    }

    public String getSQLKeywords() throws SQLException
    {

        return null;
    }

    public String getNumericFunctions() throws SQLException
    {

        return null;
    }

    public String getStringFunctions() throws SQLException
    {

        return null;
    }

    public String getSystemFunctions() throws SQLException
    {

        return null;
    }

    public String getTimeDateFunctions() throws SQLException
    {
        return "date";
    }

    public String getSearchStringEscape() throws SQLException
    {

        return null;
    }

    public String getExtraNameCharacters() throws SQLException
    {

        return null;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsAlterTableWithAddColumn()
     */
    public boolean supportsAlterTableWithAddColumn() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsAlterTableWithDropColumn()
     */
    public boolean supportsAlterTableWithDropColumn() throws SQLException
    {
        return false;
    }

    public boolean supportsColumnAliasing() throws SQLException
    {

        return false;
    }

    public boolean nullPlusNonNullIsNull() throws SQLException
    {

        return false;
    }

    public boolean supportsConvert() throws SQLException
    {

        return false;
    }

    public boolean supportsConvert(int fromType, int toType) throws SQLException
    {

        return false;
    }

    public boolean supportsTableCorrelationNames() throws SQLException
    {

        return false;
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException
    {

        return false;
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException
    {

        return false;
    }

    public boolean supportsOrderByUnrelated() throws SQLException
    {

        return false;
    }

    public boolean supportsGroupBy() throws SQLException
    {

        return false;
    }

    public boolean supportsGroupByUnrelated() throws SQLException
    {

        return false;
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException
    {

        return false;
    }

    public boolean supportsLikeEscapeClause() throws SQLException
    {
        return true;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsMultipleResultSets()
     */
    public boolean supportsMultipleResultSets() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsMultipleTransactions()
     */
    public boolean supportsMultipleTransactions() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsNonNullableColumns()
     */
    public boolean supportsNonNullableColumns() throws SQLException
    {
        return true;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsMinimumSQLGrammar()
     */
    public boolean supportsMinimumSQLGrammar() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsCoreSQLGrammar()
     */
    public boolean supportsCoreSQLGrammar() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsExtendedSQLGrammar()
     */
    public boolean supportsExtendedSQLGrammar() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsANSI92EntryLevelSQL()
     */
    public boolean supportsANSI92EntryLevelSQL() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsANSI92IntermediateSQL()
     */
    public boolean supportsANSI92IntermediateSQL() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsANSI92FullSQL()
     */
    public boolean supportsANSI92FullSQL() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsIntegrityEnhancementFacility()
     */
    public boolean supportsIntegrityEnhancementFacility() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsOuterJoins()
     */
    public boolean supportsOuterJoins() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsFullOuterJoins()
     */
    public boolean supportsFullOuterJoins() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsLimitedOuterJoins()
     */
    public boolean supportsLimitedOuterJoins() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#getSchemaTerm()
     */
    public String getSchemaTerm() throws SQLException
    {
        return null;
    }

    /**
     * @see java.sql.DatabaseMetaData#getProcedureTerm()
     */
    public String getProcedureTerm() throws SQLException
    {
        return null;
    }

    /**
     * @see java.sql.DatabaseMetaData#getCatalogTerm()
     */
    public String getCatalogTerm() throws SQLException
    {
        return "database";
    }

    /**
     * @see java.sql.DatabaseMetaData#isCatalogAtStart()
     */
    public boolean isCatalogAtStart() throws SQLException
    {
        return true;
    }

    /**
     * @see java.sql.DatabaseMetaData#getCatalogSeparator()
     */
    public String getCatalogSeparator() throws SQLException
    {
        return ".";
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsSchemasInDataManipulation()
     */
    public boolean supportsSchemasInDataManipulation() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsSchemasInProcedureCalls()
     */
    public boolean supportsSchemasInProcedureCalls() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsSchemasInTableDefinitions()
     */
    public boolean supportsSchemasInTableDefinitions() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsSchemasInIndexDefinitions()
     */
    public boolean supportsSchemasInIndexDefinitions() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsSchemasInPrivilegeDefinitions()
     */
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsCatalogsInDataManipulation()
     */
    public boolean supportsCatalogsInDataManipulation() throws SQLException
    {
        return true;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsCatalogsInProcedureCalls()
     */
    public boolean supportsCatalogsInProcedureCalls() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsCatalogsInTableDefinitions()
     */
    public boolean supportsCatalogsInTableDefinitions() throws SQLException
    {
        return false;
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException
    {

        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsCatalogsInPrivilegeDefinitions()
     */
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsPositionedDelete()
     */
    public boolean supportsPositionedDelete() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsPositionedUpdate()
     */
    public boolean supportsPositionedUpdate() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsSelectForUpdate()
     */
    public boolean supportsSelectForUpdate() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsStoredProcedures()
     */
    public boolean supportsStoredProcedures() throws SQLException
    {
        return false;
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException
    {

        return false;
    }

    public boolean supportsSubqueriesInExists() throws SQLException
    {

        return false;
    }

    public boolean supportsSubqueriesInIns() throws SQLException
    {

        return false;
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException
    {

        return false;
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException
    {

        return false;
    }

    public boolean supportsUnion() throws SQLException
    {

        return false;
    }

    public boolean supportsUnionAll() throws SQLException
    {

        return false;
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException
    {

        return false;
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException
    {

        return false;
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException
    {

        return false;
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException
    {

        return false;
    }

    public int getMaxBinaryLiteralLength() throws SQLException
    {

        return 0;
    }

    public int getMaxCharLiteralLength() throws SQLException
    {

        return 0;
    }

    public int getMaxColumnNameLength() throws SQLException
    {

        return 0;
    }

    public int getMaxColumnsInGroupBy() throws SQLException
    {

        return 0;
    }

    public int getMaxColumnsInIndex() throws SQLException
    {

        return 0;
    }

    public int getMaxColumnsInOrderBy() throws SQLException
    {

        return 0;
    }

    public int getMaxColumnsInSelect() throws SQLException
    {

        return 0;
    }

    public int getMaxColumnsInTable() throws SQLException
    {

        return 0;
    }

    /**
     * @see java.sql.DatabaseMetaData#getMaxConnections()
     */
    public int getMaxConnections() throws SQLException
    {
        return 0;
    }

    public int getMaxCursorNameLength() throws SQLException
    {

        return 0;
    }

    public int getMaxIndexLength() throws SQLException
    {

        return 0;
    }

    public int getMaxSchemaNameLength() throws SQLException
    {

        return 0;
    }

    public int getMaxProcedureNameLength() throws SQLException
    {

        return 0;
    }

    public int getMaxCatalogNameLength() throws SQLException
    {
        return 0;
    }

    public int getMaxRowSize() throws SQLException
    {

        return 0;
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
    {

        return false;
    }

    public int getMaxStatementLength() throws SQLException
    {

        return 0;
    }

    public int getMaxStatements() throws SQLException
    {

        return 0;
    }

    /**
     * @see java.sql.DatabaseMetaData#getMaxTableNameLength()
     */
    public int getMaxTableNameLength() throws SQLException
    {
        /*
        * The maximum size of a collection name is 128 characters (including the name of the db and indexes).
        * It is probably best to keep it under 80/90 chars.
        */
        return 90;
    }

    /**
     * @see java.sql.DatabaseMetaData#getMaxTablesInSelect()
     */
    public int getMaxTablesInSelect() throws SQLException
    {
        // Cassandra collections are represented as SQL tables in this driver. Cassandra doesn't support joins.
        return 1;
    }

    public int getMaxUserNameLength() throws SQLException
    {

        return 0;
    }

    /**
     * @see java.sql.DatabaseMetaData#getDefaultTransactionIsolation()
     */
    public int getDefaultTransactionIsolation() throws SQLException
    {
        return Connection.TRANSACTION_NONE;
    }

    /**
     * Cassandra doesn't support transactions, but document updates are atomic.
     *
     * @see java.sql.DatabaseMetaData#supportsTransactions()
     */
    public boolean supportsTransactions() throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsTransactionIsolationLevel(int)
     */
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException
    {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsDataDefinitionAndDataManipulationTransactions()
     */
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException
    {
        return false;
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException
    {

        return false;
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException
    {

        return false;
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException
    {

        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#getProcedures(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getProcedures(String catalogName, String schemaPattern, String procedureNamePattern)
            throws SQLException
    {
        ArrayResultSet retVal = new ArrayResultSet();
        retVal.setColumnNames(new String[] { "PROCEDURE_CAT", "PROCEDURE_SCHEMA", "PROCEDURE_NAME", "REMARKS",
                "PROCEDURE_TYPE", "SPECIFIC_NAME" });
        return retVal;
    }

    /**
     * @see java.sql.DatabaseMetaData#getProcedureColumns(java.lang.String, java.lang.String, java.lang.String,
     *      java.lang.String)
     */
    @Override
    public ResultSet getProcedureColumns(String catalogName, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) throws SQLException
    {
        return EMPTY_RESULT_SET;
    }

    /**
     * @see java.sql.DatabaseMetaData#getTableTypes()
     */
    @Override
    public ResultSet getTableTypes() throws SQLException
    {
        ArrayResultSet result = new ArrayResultSet();
        result.addRow(new String[] { "COLLECTION" });
        return result;
    }


    @Override
    public ResultSet getColumnPrivileges(String catalogName, String schemaName, String table, String columnNamePattern)
            throws SQLException
    {

        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalogName, String schemaPattern, String tableNamePattern)
            throws SQLException
    {

        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalogName, String schemaName, String table, int scope,
                                          boolean nullable) throws SQLException
    {

        return null;
    }

    /**
     * @see java.sql.DatabaseMetaData#getVersionColumns(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public ResultSet getVersionColumns(String catalogName, String schemaName, String table) throws SQLException
    {
        return EMPTY_RESULT_SET;
    }

    @Override
    public ResultSet getExportedKeys(String catalogName, String schemaName, String tableNamePattern) throws SQLException {
        final ArrayResultSet result = new ArrayResultSet();
        result.setColumnNames(new String[]{"PKTABLE_CAT", "PKTABLE_SCHEMA", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM",
                "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"});
        return result;
    }

    /**
     * @see java.sql.DatabaseMetaData#getExportedKeys(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public ResultSet getImportedKeys(String catalogName, String schemaName, String tableNamePattern ) throws SQLException
    {
        ArrayResultSet result = new ArrayResultSet();
        result.setColumnNames(new String[]{"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM",
                "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"});

        return result;
    }


    /**
     * @see java.sql.DatabaseMetaData#getCrossReference(java.lang.String, java.lang.String, java.lang.String,
     *      java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException
    {
        return EMPTY_RESULT_SET;
    }


    /**
     * @see java.sql.DatabaseMetaData#supportsResultSetType(int)
     */
    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsResultSetConcurrency(int, int)
     */
    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException	{
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException
    {

        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException	{
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException	{
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException	{
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#getUDTs(java.lang.String, java.lang.String, java.lang.String, int[])
     */
    @Override
    public ResultSet getUDTs(String catalogName, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException	{
        ArrayResultSet retVal = new ArrayResultSet();
        retVal.setColumnNames(new String[] { "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE",
                "REMARKS", "BASE_TYPE", });
        return retVal;
    }

    /**
     * @see java.sql.DatabaseMetaData#getConnection()
     */
    @Override
    public Connection getConnection() throws SQLException {
        return con;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsSavepoints()
     */
    @Override
    public boolean supportsSavepoints() throws SQLException	{
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsMultipleOpenResults()
     */
    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsGetGeneratedKeys()
     */
    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#getSuperTypes(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public ResultSet getSuperTypes(String catalogName, String schemaPattern, String typeNamePattern)
            throws SQLException
    {
        ArrayResultSet retVal = new ArrayResultSet();
        retVal.setColumnNames(new String[] { "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT",
                "SUPERTYPE_SCHEM", "SUPERTYPE_NAME" });
        return retVal;
    }

    /**
     * @see java.sql.DatabaseMetaData#getSuperTables(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public ResultSet getSuperTables(String catalogName, String schemaPattern, String tableNamePattern)
            throws SQLException
    {
        ArrayResultSet retVal = new ArrayResultSet();
        retVal.setColumnNames(new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME" });
        return retVal;
    }

    /**
     * @see java.sql.DatabaseMetaData#getAttributes(java.lang.String, java.lang.String, java.lang.String,
     *      java.lang.String)
     */
    @Override
    public ResultSet getAttributes(String catalogName, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        ArrayResultSet retVal = new ArrayResultSet();
        retVal.setColumnNames(new String[] { "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME", "DATA_TYPE",
                "ATTR_TYPE_NAME", "ATTR_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
                "ATTR_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
                "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE" });
        return retVal;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsResultSetHoldability(int)
     */
    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#getResultSetHoldability()
     */
    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    /**
     * @see java.sql.DatabaseMetaData#getDatabaseMajorVersion()
     */
    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return Integer.parseInt((getDatabaseProductVersion().split("\\."))[0]);
    }

    /**
     * @see java.sql.DatabaseMetaData#getDatabaseMinorVersion()
     */
    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return Integer.parseInt((getDatabaseProductVersion().split("\\."))[1]);
    }

    /**
     * @see java.sql.DatabaseMetaData#getJDBCMajorVersion()
     */
    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 1;
    }

    /**
     * @see java.sql.DatabaseMetaData#getJDBCMinorVersion()
     */
    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    /**
     * @see java.sql.DatabaseMetaData#getSQLStateType()
     */
    @Override
    public int getSQLStateType() throws SQLException {
        return DatabaseMetaData.sqlStateXOpen;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException	{
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsStatementPooling()
     */
    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    /**
     * @see java.sql.DatabaseMetaData#getRowIdLifetime()
     */
    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException	{
        return null;
    }

    /**
     * @see java.sql.DatabaseMetaData#getSchemas(java.lang.String, java.lang.String)
     */
    @Override
    public ResultSet getSchemas(String catalogName, String schemaPattern) throws SQLException {
        ArrayResultSet retVal = new ArrayResultSet();
        retVal.setColumnNames(new String[] { "TABLE_SCHEM", "TABLE_CATALOG" });
        return retVal;
    }

    /**
     * @see java.sql.DatabaseMetaData#supportsStoredFunctionsUsingCallSyntax()
     */
    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException	{
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException	{
        return null;
    }

    @Override
    public ResultSet getFunctions(String catalogName, String schemaPattern, String functionNamePattern)
            throws SQLException	{
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalogName, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalogName, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }


    public static int getJavaTypeByName(String typeName) {
        int type = Types.VARCHAR;
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
        return type;
    }

}
