package com.dbschema.codec.jlong;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class FloatCodec extends BaseLongCodec {

    public static final FloatCodec INSTANCE = new FloatCodec();

    private FloatCodec() {
        super(DataType.cdouble());
    }

    @Override
    int getNumberOfBytes() {
        return 4;
    }

    @Override
    void serializeNoBoxingInner(long value, ByteBuffer bb) throws InvalidTypeException {
        bb.putFloat(value);
    }

    @Override
    long deserializeNoBoxingInner(ByteBuffer bytes) {
        return (long) bytes.getFloat();
    }
}
