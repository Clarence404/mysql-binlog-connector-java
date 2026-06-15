package com.github.shyiko.mysql.binlog.event;

import java.io.InputStream;
import java.util.ArrayList;


public class TransactionPayloadEventData implements EventData {
    private long payloadSize;
    private long uncompressedSize;
    private int compressionType;
    private byte[] payload;
    private transient InputStream payloadInputStream;
    private ArrayList<Event> uncompressedEvents = new ArrayList<Event>();

    public ArrayList<Event> getUncompressedEvents() {
        return uncompressedEvents;
    }

    public void setUncompressedEvents(ArrayList<Event> uncompressedEvents) {
        this.uncompressedEvents = uncompressedEvents;
    }

    public int getPayloadSize() {
        if (payloadSize > Integer.MAX_VALUE) {
            throw new IllegalStateException("Transaction payload size " + payloadSize +
                " exceeds the maximum int value");
        }
        return (int) payloadSize;
    }

    public void setPayloadSize(int payloadSize) {
        this.payloadSize = payloadSize;
    }

    public long getPayloadSizeLong() {
        return payloadSize;
    }

    public void setPayloadSize(long payloadSize) {
        this.payloadSize = payloadSize;
    }

    public long getUncompressedSize() {
        return uncompressedSize;
    }

    public void setUncompressedSize(long uncompressedSize) {
        this.uncompressedSize = uncompressedSize;
    }

    public int getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(int compressionType) {
        this.compressionType = compressionType;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public InputStream getPayloadInputStream() {
        return payloadInputStream;
    }

    public void setPayloadInputStream(InputStream payloadInputStream) {
        this.payloadInputStream = payloadInputStream;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TransactionPayloadEventData");
        sb.append("{compression_type=").append(compressionType).append(", payload_size=").append(payloadSize).append(", uncompressed_size='").append(uncompressedSize).append('\'');
        sb.append(", payload: ");
        sb.append("\n");
        for (Event e : getUncompressedEvents()) {
            sb.append(e.toString());
            sb.append("\n");
        }
        sb.append("}");
        return sb.toString();
    }
}
