package com.wisecoders.dbschema.cassandra.types;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Licensed under <a href="https://creativecommons.org/licenses/by-nd/4.0/">CC BY-ND 4.0 DEED</a>, copyright <a href="https://wisecoders.com">Wise Coders GmbH</a>, used by <a href="https://dbschema.com">DbSchema Database Designer</a>.
 * Code modifications allowed only as pull requests to the <a href="https://github.com/wise-coders/cassandra-jdbc-driver">public GIT repository</a>.
 */

public class BlobImpl implements Blob {
    private byte[] bytes;

    public BlobImpl(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public long length() {
        return bytes.length;
    }

    @Override
    public byte[] getBytes(long pos, int length) {
        byte[] newBytes = new byte[length];
        System.arraycopy(bytes, (int) pos - 1, newBytes, 0, length);
        return newBytes;
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void truncate(long len) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void free() {
        bytes = null;
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
