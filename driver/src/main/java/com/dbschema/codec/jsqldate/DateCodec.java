package com.dbschema.codec.jsqldate;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;
import java.sql.Date;

/**
 * @author Liudmila Kornilova
 **/
public class DateCodec extends TypeCodec<java.sql.Date> {

    public static final DateCodec INSTANCE = new DateCodec();
    private final TypeCodec<LocalDate> dateCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(DataType.date(), LocalDate.class);

    private DateCodec() {
        super(DataType.date(), java.sql.Date.class);
    }

    @Override
    public ByteBuffer serialize(Date value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null) return null;
        LocalDate localDate = LocalDate.fromMillisSinceEpoch(value.getTime());
        return dateCodec.serialize(localDate, protocolVersion);
    }

    @Override
    public Date deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null) return null;
        return new java.sql.Date(dateCodec.deserialize(bytes, protocolVersion).getMillisSinceEpoch());
    }

    @Override
    public Date parse(String value) throws InvalidTypeException {
        throw new RuntimeException("Not supported");
    }

    @Override
    public String format(Date value) throws InvalidTypeException {
        throw new RuntimeException("Not supported");
    }
}
