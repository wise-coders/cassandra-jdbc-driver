package com.dbschema.codec.jtimeuuid;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author Liudmila Kornilova
 **/
public class StringCodec extends TypeCodec<String> {
    public static final StringCodec INSTANCE = new StringCodec();
    private final TypeCodec<UUID> timeuuidCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(DataType.timeuuid(), UUID.class);

    private StringCodec() {
        super(DataType.timeuuid(), String.class);
    }

    @Override
    public ByteBuffer serialize(String value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null) return null;
        return timeuuidCodec.serialize(UUID.fromString(value), protocolVersion);
    }

    @Override
    public String  deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null) return null;
        return timeuuidCodec.deserialize(bytes, protocolVersion).toString();
    }

    @Override
    public String parse(String value) throws InvalidTypeException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public String format(String value) throws InvalidTypeException {
        throw new RuntimeException("Not implemented");
    }
}
