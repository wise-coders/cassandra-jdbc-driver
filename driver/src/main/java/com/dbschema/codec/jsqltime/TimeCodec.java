package com.dbschema.codec.jsqltime;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;
import java.sql.Time;

/**
 * @author Liudmila Kornilova
 **/
public class TimeCodec extends TypeCodec<java.sql.Time> {
    public static final TimeCodec INSTANCE = new TimeCodec();
    private final TypeCodec<Long> timeCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(DataType.time(), Long.class);

    private TimeCodec() {
        super(DataType.time(), java.sql.Time.class);
    }

    @Override
    public ByteBuffer serialize(Time value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null) return null;
        long milliseconds = value.getTime();
        return timeCodec.serialize(milliseconds * 1000000, protocolVersion);
    }

    @Override
    public Time deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null) return null;
        long nanoseconds = timeCodec.deserialize(bytes, protocolVersion);
        return new Time(nanoseconds / 1000000);
    }

    @Override
    public Time parse(String value) throws InvalidTypeException {
        throw new RuntimeException("Not supported");
    }

    @Override
    public String format(Time value) throws InvalidTypeException {
        throw new RuntimeException("Not supported");
    }
}
