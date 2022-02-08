package com.dbschema.types;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;


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
