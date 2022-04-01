package com.wisecoders.dbschema.cassandra;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;

/**
 * Copyright Wise Coders GmbH. The Cassandra JDBC driver is build to be used with DbSchema Database Designer https://dbschema.com
 * Free to use by everyone, code modifications allowed only to
 * the public repository https://github.com/wise-coders/cassandra-jdbc-driver
 */


public class CassandraStatement extends CassandraBaseStatement {

    private final CassandraConnection connection;

    CassandraStatement( CassandraConnection connection) {
        super(connection.getSession());
        this.connection = connection;
    }


    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        try {
            result = new CassandraResultSet(this, session.execute(sql));
            return result;
        } catch (SyntaxError ex) {
            ResultSet rs = connection.executeDescribeCommand( sql );
            if ( rs != null ){
                return rs;
            }
            throw new SQLSyntaxErrorException(ex.getMessage(), ex);
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        try {
            result = new CassandraResultSet(this, session.execute(sql));
            if (result.isQuery()) {
                throw new SQLException("Not an update statement");
            }
            return 1;
        } catch (SyntaxError ex) {
            throw new SQLSyntaxErrorException(ex.getMessage(), ex);
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        try {
            return executeInner(session.execute(sql), true);
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return result;
    }

    @Override
    public void addBatch(String sql) {
        if (batchStatementBuilder == null) {
            batchStatementBuilder = BatchStatement.builder(BatchType.LOGGED);
        }
        batchStatementBuilder.addStatement(SimpleStatement.newInstance(sql));
    }

    @Override
    public void clearBatch() {
        batchStatementBuilder = null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}