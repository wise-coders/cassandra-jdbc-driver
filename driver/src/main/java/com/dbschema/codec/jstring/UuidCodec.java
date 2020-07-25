package com.dbschema.codec.jstring;

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
public class UuidCodec extends TypeCodec<String> {
    public static final UuidCodec INSTANCE = new UuidCodec();
    private final TypeCodec<UUID> uuidCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(DataType.uuid(), UUID.class);

    private UuidCodec() {
        super(DataType.uuid(), String.class);
    }

    @Override
    public ByteBuffer serialize(String value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null) return null;
        try {
            UUID uuid = UUID.fromString(value);
            return uuidCodec.serialize(uuid, protocolVersion);
        } catch (IllegalArgumentException e) {
            throw new InvalidTypeException(e.getMessage());
        }
    }

    @Override
    public String deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null) return null;
        return uuidCodec.deserialize(bytes, protocolVersion).toString();
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
