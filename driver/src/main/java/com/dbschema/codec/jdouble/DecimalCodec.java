package com.dbschema.codec.jdouble;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class DecimalCodec extends TypeCodec<Double> {
    public final static DecimalCodec INSTANCE = new DecimalCodec();
    private final TypeCodec<BigDecimal> decimalCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(DataType.decimal(), BigDecimal.class);

    private DecimalCodec() {
        super(DataType.decimal(), Double.class);
    }

    @Override
    public ByteBuffer serialize(Double value, ProtocolVersion protocolVersion) {
        return decimalCodec.serialize(new BigDecimal(value), protocolVersion);
    }

    @Override
    public Double deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        BigDecimal bigDecimal = decimalCodec.deserialize(bytes, protocolVersion);
        if (bigDecimal == null) return null;
        return bigDecimal.doubleValue();
    }

    @Override
    public Double parse(String value) throws InvalidTypeException {
        throw new RuntimeException("Not supported");
    }

    @Override
    public String format(Double value) throws InvalidTypeException {
        throw new RuntimeException("Not supported");
    }
}
