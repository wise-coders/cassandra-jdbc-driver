package com.dbschema.codec.javalong;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class VarintCodec extends TypeCodec<Long> {

    public static final VarintCodec INSTANCE = new VarintCodec();

    private VarintCodec() {
        super(DataType.varint(), Long.class);
    }

    @Override
    public Long parse(String value) {
        try {
            return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                    ? null
                    : new Long(value);
        } catch (NumberFormatException e) {
            throw new InvalidTypeException(
                    String.format("Cannot parse long value from \"%s\"", value), e);
        }
    }

    @Override
    public String format(Long value) {
        if (value == null) return "NULL";
        return value.toString();
    }

    @Override
    public ByteBuffer serialize(Long value, ProtocolVersion protocolVersion) {
        if (value == null) return null;
        BigInteger bigInteger = new BigInteger(value.toString());
        return ByteBuffer.wrap(bigInteger.toByteArray());
    }

    @Override
    public Long deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return bytes == null || bytes.remaining() == 0 ? null : new BigInteger(Bytes.getArray(bytes)).longValue();
    }
}
