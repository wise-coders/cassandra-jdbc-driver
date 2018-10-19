package com.dbschema.codec.javalong;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;

public class IntCodec extends BaseLongCodec {

    public static final IntCodec INSTANCE = new IntCodec();

    private IntCodec() {
        super(DataType.cint());
    }

    @Override
    int getNumberOfBytes() {
        return 4;
    }

    @Override
    void serializeNoBoxingInner(long value, ByteBuffer bb) throws InvalidTypeException {
        if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
            throw new InvalidTypeException("Long value " + value + " does not fit into cql type int");
        }
        bb.putInt((int) value);
    }

    @Override
    long deserializeNoBoxingInner(ByteBuffer bytes) {
        return bytes.getInt();
    }
}
