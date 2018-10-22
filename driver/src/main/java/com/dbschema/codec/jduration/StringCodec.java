package com.dbschema.codec.jduration;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class StringCodec extends TypeCodec<String> {
    public static final StringCodec INSTANCE = new StringCodec();
    private final TypeCodec<Duration> durationCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(DataType.duration(), Duration.class);

    private StringCodec() {
        super(DataType.duration(), String.class);
    }

    @Override
    public ByteBuffer serialize(String value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null) return null;
        Duration duration = Duration.from(value);
        return durationCodec.serialize(duration, protocolVersion);
    }

    @Override
    public String deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null) return null;
        return durationCodec.deserialize(bytes, protocolVersion).toString();
    }

    @Override
    public String parse(String value) throws InvalidTypeException {
        throw new RuntimeException("Not supported");
    }

    @Override
    public String format(String value) throws InvalidTypeException {
        throw new RuntimeException("Not supported");
    }
}
