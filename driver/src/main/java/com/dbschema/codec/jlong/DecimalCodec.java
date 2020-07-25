package com.dbschema.codec.jlong;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class DecimalCodec extends TypeCodec<Long> {

    public static final DecimalCodec INSTANCE = new DecimalCodec();

    private DecimalCodec() {
        super(DataType.decimal(), Long.class);
    }

    @Override
    public Long parse(String value) {
        try {
            return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                    ? null
                    : new Long(value);
        } catch (NumberFormatException e) {
            throw new InvalidTypeException(
                    String.format("Cannot parse long value from \"%s\"", value));
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
        BigDecimal bigDecimal = new BigDecimal(value);
        BigInteger bi = bigDecimal.unscaledValue();
        int scale = bigDecimal.scale();
        byte[] bibytes = bi.toByteArray();

        ByteBuffer bytes = ByteBuffer.allocate(4 + bibytes.length);
        bytes.putInt(scale);
        bytes.put(bibytes);
        bytes.rewind();
        return bytes;
    }

    @Override
    public Long deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() == 0) return null;
        if (bytes.remaining() < 4)
            throw new InvalidTypeException(
                    "Invalid decimal value, expecting at least 4 bytes but got " + bytes.remaining());

        bytes = bytes.duplicate();
        int scale = bytes.getInt();
        byte[] bibytes = new byte[bytes.remaining()];
        bytes.get(bibytes);

        BigInteger bi = new BigInteger(bibytes);
        return new BigDecimal(bi, scale).longValue();
    }
}
