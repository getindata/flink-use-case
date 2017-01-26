package com.getindata;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class EventCoder extends AtomicCoder<Event> {
    @Override
    public void encode(Event value, OutputStream outStream, Context context) throws CoderException, IOException {
        outStream.write(com.getindata.serialization.EventCoderUtils.encode(value));
    }

    @Override
    public Event decode(InputStream inStream, Context context) throws CoderException, IOException {
        byte[] bytes = StreamUtils.getBytes(inStream);
        return com.getindata.serialization.EventCoderUtils.decodeEvent(bytes);
    }
}
