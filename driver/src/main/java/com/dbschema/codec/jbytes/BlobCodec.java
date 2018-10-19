package com.dbschema.codec.jbytes;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;

import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class BlobCodec extends TypeCodec<byte[]> {

    public static final BlobCodec INSTANCE = new BlobCodec();

    private BlobCodec() {
        super(DataType.blob(), byte[].class);
    }

    @Override
    public ByteBuffer serialize(byte[] value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return value == null ? null : ByteBuffer.wrap(value);
    }

    @Override
    public byte[] deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return bytes == null ? null : Bytes.getArray(bytes);
    }

    @Override
    public byte[] parse(String value) throws InvalidTypeException {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                ? null
                : Bytes.getArray(Bytes.fromHexString(value));
    }

    @Override
    public String format(byte[] value) throws InvalidTypeException {
        if (value == null) return "NULL";
        return Bytes.toHexString(value);
    }
}
