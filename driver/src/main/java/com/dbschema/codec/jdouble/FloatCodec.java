package com.dbschema.codec.jdouble;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class FloatCodec extends TypeCodec.PrimitiveDoubleCodec {
    public final static FloatCodec INSTANCE = new FloatCodec();

    private FloatCodec() {
        super(DataType.cfloat());
    }

    @Override
    public ByteBuffer serializeNoBoxing(double value, ProtocolVersion protocolVersion) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putFloat((float) value);
        bb.flip();
        return bb;
    }

    @Override
    public double deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes.remaining() != 4)
            throw new InvalidTypeException(
                    "Invalid 32-bits float value, expecting 4 bytes but got " + bytes.remaining());

        return bytes.getFloat();
    }

    @Override
    public Double parse(String value) throws InvalidTypeException {
        try {
            return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                    ? null
                    : Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new InvalidTypeException(
                    String.format("Cannot parse 64-bits double value from \"%s\"", value));
        }
    }

    @Override
    public String format(Double value) throws InvalidTypeException {
        if (value == null) return "NULL";
        return Double.toString(value);
    }
}
