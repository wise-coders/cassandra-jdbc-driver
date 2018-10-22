package com.dbschema.codec.jinet;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class StringCodec extends TypeCodec<String> {
    public static final StringCodec INSTANCE = new StringCodec();
    private final TypeCodec<InetAddress> inetCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(DataType.inet(), InetAddress.class);

    private StringCodec() {
        super(DataType.inet(), String.class);
    }

    @Override
    public ByteBuffer serialize(String value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null) return null;
        try {
            return inetCodec.serialize(InetAddress.getByName(value), protocolVersion);
        } catch (UnknownHostException e) {
            throw new InvalidTypeException(e.getMessage());
        }
    }

    @Override
    public String deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null) return null;
        return inetCodec.deserialize(bytes, protocolVersion).getHostAddress();
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
