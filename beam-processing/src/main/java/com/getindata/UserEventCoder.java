package com.getindata;

import com.getindata.serialization.EventCoderUtils;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.util.VarInt;

import java.io.*;

public class UserEventCoder extends AtomicCoder<UserEvent> {
    @Override
    public void encode(UserEvent value, OutputStream outStream, Context context) throws CoderException, IOException {
        final byte[] bytes = EventCoderUtils.encode(value);
        if (context.isWholeStream) {
            outStream.write(bytes);
        } else {
            final DataOutputStream dos = new DataOutputStream(outStream);
            VarInt.encode(bytes.length, dos);
            dos.write(bytes);
        }
    }

    @Override
    public UserEvent decode(InputStream inStream, Context context) throws CoderException, IOException {
        if (context.isWholeStream) {
            byte[] bytes = StreamUtils.getBytes(inStream);
            return EventCoderUtils.decodeUserEvent(bytes);
        } else {
            DataInputStream dis = new DataInputStream(inStream);
            int len = VarInt.decodeInt(dis);
            byte[] bytes = new byte[len];
            dis.readFully(bytes);
            return EventCoderUtils.decodeUserEvent(bytes);
        }
    }

    public static UserEventCoder of() {
        return new UserEventCoder();
    }
}
