package com.dbschema.codec.jbiginteger;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class LongCodec extends TypeCodec<BigDecimal> {
    public static final LongCodec INSTANCE = new LongCodec();

    private LongCodec() {
        super(DataType.bigint(), BigDecimal.class);
    }

    @Override
    public ByteBuffer serialize(BigDecimal value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null) return null;
        ByteBuffer bb = ByteBuffer.allocate(8);
        try {
            long l = value.longValueExact();
            bb.putLong(l);
        } catch (ArithmeticException e) {
            throw new InvalidTypeException("BigInteger value " + value + " does not fit into cql type long", e);
        }
        bb.flip();
        return bb;
    }

    @Override
    public BigDecimal deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return bytes == null || bytes.remaining() == 0 ? null : deserializeNoBoxing(bytes);
    }

    private BigDecimal deserializeNoBoxing(ByteBuffer bytes) {
        if (bytes.remaining() != 8) {
            throw new InvalidTypeException("Invalid value, expecting 8 bytes but got " + bytes.remaining());
        }
        return new BigDecimal(Long.toString(bytes.getLong()));
    }

    @Override
    public BigDecimal parse(String value) throws InvalidTypeException {
        try {
            return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                    ? null
                    : new BigDecimal(value);
        } catch (NumberFormatException e) {
            throw new InvalidTypeException(
                    String.format("Cannot parse 64-bits long value from \"%s\"", value));
        }
    }

    @Override
    public String format(BigDecimal value) throws InvalidTypeException {
        if (value == null) return "NULL";
        return Long.toString(value.longValue());
    }
}
