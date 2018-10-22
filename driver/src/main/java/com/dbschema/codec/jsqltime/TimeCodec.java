package com.dbschema.codec.jsqltime;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;
import java.sql.Time;

/**
 * @author Liudmila Kornilova
 **/
public class TimeCodec extends TypeCodec<java.sql.Time> {
    public static final TimeCodec INSTANCE = new TimeCodec();

    private TimeCodec() {
        super(DataType.time(), java.sql.Time.class);
    }

    @Override
    public ByteBuffer serialize(Time value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null) return null;
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(0, value.getTime());
        return bb;
    }

    @Override
    public Time deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null) return null;
        return new Time(bytes.getLong());
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
