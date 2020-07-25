package com.dbschema.codec.jlong;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public abstract class BaseLongCodec extends TypeCodec.PrimitiveLongCodec {
    BaseLongCodec(DataType cqlType) {
        super(cqlType);
    }

    abstract int getNumberOfBytes();

    abstract void serializeNoBoxingInner(long value, ByteBuffer bb) throws InvalidTypeException;

    abstract long deserializeNoBoxingInner(ByteBuffer bytes);

    @Override
    public Long parse(String value) {
        try {
            return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                    ? null
                    : Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new InvalidTypeException(String.format("Cannot parse value from \"%s\"", value));
        }
    }

    @Override
    public String format(Long value) {
        if (value == null) return "NULL";
        return Long.toString(value);
    }

    @Override
    public ByteBuffer serializeNoBoxing(long value, ProtocolVersion protocolVersion) {
        ByteBuffer bb = ByteBuffer.allocate(getNumberOfBytes());
        serializeNoBoxingInner(value, bb);
        bb.flip();
        return bb;
    }

    @Override
    public long deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() == 0) return 0;
        if (bytes.remaining() != getNumberOfBytes())
            throw new InvalidTypeException(
                    "Invalid value, expecting " + getNumberOfBytes() + " bytes but got " + bytes.remaining());

        return deserializeNoBoxingInner(bytes);
    }
}
