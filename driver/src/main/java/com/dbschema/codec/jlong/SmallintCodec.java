package com.dbschema.codec.jlong;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class SmallintCodec extends BaseLongCodec {

    public static final SmallintCodec INSTANCE = new SmallintCodec();

    private SmallintCodec() {
        super(DataType.smallint());
    }

    @Override
    int getNumberOfBytes() {
        return 2;
    }

    @Override
    void serializeNoBoxingInner(long value, ByteBuffer bb) throws InvalidTypeException {
        if (value > Short.MAX_VALUE || value < Short.MIN_VALUE) {
            throw new InvalidTypeException("Long value " + value + " does not fit into cql type smallint");
        }
        bb.putShort((short) value);
    }

    @Override
    long deserializeNoBoxingInner(ByteBuffer bytes) {
        return bytes.getShort();
    }
}
