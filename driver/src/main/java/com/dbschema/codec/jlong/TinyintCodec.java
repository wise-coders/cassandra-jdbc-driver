package com.dbschema.codec.jlong;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class TinyintCodec extends BaseLongCodec {

    public static final TinyintCodec INSTANCE = new TinyintCodec();

    private TinyintCodec() {
        super(DataType.tinyint());
    }

    @Override
    int getNumberOfBytes() {
        return 1;
    }

    @Override
    void serializeNoBoxingInner(long value, ByteBuffer bb) throws InvalidTypeException {
        if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE) {
            throw new InvalidTypeException("Long value " + value + " does not fit into cql type tinyint");
        }
        bb.put((byte) value);
    }

    @Override
    long deserializeNoBoxingInner(ByteBuffer bytes) {
        return bytes.get();
    }
}
