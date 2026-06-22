/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.luben.zstd.ZstdInputStream;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;

/**
 * @author <a href="mailto:somesh.malviya@booking.com">Somesh Malviya</a>
 * @author <a href="mailto:debjeet.sarkar@booking.com">Debjeet Sarkar</a>
 * @author <a href="mailto:pratik.pandey@booking.com">Pratik Pandey</a>
 */
public class TransactionPayloadEventDataDeserializer implements EventDataDeserializer<TransactionPayloadEventData> {
    public static final int OTW_PAYLOAD_HEADER_END_MARK = 0;
    public static final int OTW_PAYLOAD_SIZE_FIELD = 1;
    public static final int OTW_PAYLOAD_COMPRESSION_TYPE_FIELD = 2;
    public static final int OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD = 3;
    public static final int COMPRESSION_TYPE_ZSTD = 0;
    public static final int COMPRESSION_TYPE_NONE = 255;

    private EventDeserializer.CompatibilityMode[] compatibilityModes = new EventDeserializer.CompatibilityMode[0];

    void setCompatibilityMode(EnumSet<EventDeserializer.CompatibilityMode> compatibilitySet) {
        compatibilityModes = compatibilitySet.toArray(
            new EventDeserializer.CompatibilityMode[compatibilitySet.size()]
        );
    }

    @Override
    public TransactionPayloadEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        return deserialize(inputStream, true);
    }

    TransactionPayloadEventData deserialize(ByteArrayInputStream inputStream, boolean materializePayload)
            throws IOException {
        TransactionPayloadEventData eventData = new TransactionPayloadEventData();
        eventData.setCompressionType(COMPRESSION_TYPE_NONE);
        // Read the header fields from the event data
        while (inputStream.available() > 0) {
            int fieldType = 0;
            int fieldLen = 0;
            // Read the type of the field
            if (inputStream.available() >= 1) {
                fieldType = inputStream.readPackedInteger();
            }
            // We have reached the end of the Event Data Header
            if (fieldType == OTW_PAYLOAD_HEADER_END_MARK) {
                break;
            }
            // Read the size of the field
            if (inputStream.available() >= 1) {
                fieldLen = inputStream.readPackedInteger();
            }
            switch (fieldType) {
                case OTW_PAYLOAD_SIZE_FIELD:
                    // Fetch the payload (compressed) size
                    eventData.setPayloadSize(inputStream.readPackedLong());
                    break;
                case OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
                    // Fetch the compression type
                    eventData.setCompressionType(inputStream.readPackedInteger());
                    break;
                case OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
                    // Fetch the uncompressed size (may exceed 2GB; inner events are streamed)
                    eventData.setUncompressedSize(inputStream.readPackedLong());
                    break;
                default:
                    // Ignore unrecognized field
                    inputStream.read(fieldLen);
                    break;
            }
        }
        if (eventData.getUncompressedSize() == 0) {
            // Default the uncompressed to the payload size
            eventData.setUncompressedSize(eventData.getPayloadSizeLong());
        }

        // Fail fast on an unsupported compression type, before consuming the payload, so it
        // surfaces as a clean deserialization failure rather than later during inner-event streaming.
        requireSupportedCompressionType(eventData.getCompressionType());
        if (eventData.getPayloadSizeLong() < 0) {
            throw new IOException("Invalid negative transaction payload size: " +
                eventData.getPayloadSizeLong());
        }

        if (materializePayload && eventData.getPayloadSizeLong() <= Integer.MAX_VALUE) {
            eventData.setPayload(inputStream.read((int) eventData.getPayloadSizeLong()));
        } else {
            eventData.setPayloadInputStream(new BoundedInputStream(inputStream, eventData.getPayloadSizeLong()));
        }

        // Inner events are intentionally NOT materialized here: a compressed transaction is bounded
        // by the transaction size, not by any per-event limit, so eagerly parsing every inner event
        // into a list would put the whole transaction's object graph on the heap at once. They are
        // surfaced one at a time through iterator(); uncompressedEvents stays empty.
        return eventData;
    }

    /**
     * Opens a single-pass cursor over the transaction's inner events, decompressing the payload
     * incrementally. Peak memory is bounded by the decompressor's buffers plus one inner event, so a
     * transaction whose compressed or uncompressed image exceeds the 2GB {@code byte[]} limit is
     * streamed through instead of being materialized whole. The returned iterator must be
     * {@link InnerEventIterator#close() closed} to release the native decompressor.
     */
    public InnerEventIterator iterator(TransactionPayloadEventData eventData) throws IOException {
        EventDeserializer innerEventDeserializer = new EventDeserializer();
        setCompatibilityMode(innerEventDeserializer);
        ByteArrayInputStream stream = new ByteArrayInputStream(getDecompressedInputStream(eventData));
        return new InnerEventIterator(innerEventDeserializer, stream, eventData.getUncompressedSize());
    }

    private static void requireSupportedCompressionType(int compressionType) throws IOException {
        if (compressionType != COMPRESSION_TYPE_ZSTD && compressionType != COMPRESSION_TYPE_NONE) {
            throw new IOException("Unsupported binlog_transaction_compression type: " +
                compressionType + " (only ZSTD and NONE are supported)");
        }
    }

    private InputStream getDecompressedInputStream(TransactionPayloadEventData eventData) throws IOException {
        InputStream payloadInputStream = eventData.getPayloadInputStream();
        if (payloadInputStream == null) {
            byte[] payload = eventData.getPayload();
            if (payload == null) {
                throw new IOException("Transaction payload is not available");
            }
            payloadInputStream = new java.io.ByteArrayInputStream(payload);
        }
        switch (eventData.getCompressionType()) {
            case COMPRESSION_TYPE_ZSTD:
                return new ZstdInputStream(payloadInputStream);
            case COMPRESSION_TYPE_NONE:
                return payloadInputStream;
            default:
                throw new IOException("Unsupported binlog_transaction_compression type: " +
                    eventData.getCompressionType() + " (only ZSTD and NONE are supported)");
        }
    }

    /**
     * Presents exactly {@code length} bytes from an underlying stream without owning/closing it.
     */
    static final class BoundedInputStream extends InputStream {
        private final InputStream inputStream;
        private long remaining;

        BoundedInputStream(InputStream inputStream, long length) {
            this.inputStream = inputStream;
            this.remaining = length;
        }

        @Override
        public int read() throws IOException {
            if (remaining == 0) {
                return -1;
            }
            int read = inputStream.read();
            if (read == -1) {
                throw new EOFException("Unexpected end of transaction payload; " + remaining +
                    " bytes still expected");
            }
            remaining--;
            return read;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining == 0) {
                return -1;
            }
            int read = inputStream.read(b, off, (int) Math.min((long) len, remaining));
            if (read == -1) {
                throw new EOFException("Unexpected end of transaction payload; " + remaining +
                    " bytes still expected");
            }
            remaining -= read;
            return read;
        }

        @Override
        public int available() throws IOException {
            return (int) Math.min((long) inputStream.available(), Math.min(remaining, (long) Integer.MAX_VALUE));
        }

        @Override
        public void close() throws IOException {
            // Do not close the owner stream; EventDeserializer drains/skips to the event boundary.
        }
    }

    private void setCompatibilityMode(EventDeserializer eventDeserializer) {
        if (compatibilityModes.length > 0) {
            EventDeserializer.CompatibilityMode first = compatibilityModes[0];
            EventDeserializer.CompatibilityMode[] rest =
                new EventDeserializer.CompatibilityMode[compatibilityModes.length - 1];
            System.arraycopy(compatibilityModes, 1, rest, 0, rest.length);
            eventDeserializer.setCompatibilityMode(first, rest);
        }
    }

    /**
     * Single-pass cursor over the inner events of a {@link TransactionPayloadEventData}. Each
     * {@link #next()} decompresses and parses just enough of the payload to return one event;
     * {@code null} signals the end of the transaction, at which point the cursor closes itself.
     */
    public static final class InnerEventIterator implements Closeable {
        private EventDeserializer eventDeserializer;
        private ByteArrayInputStream inputStream;
        private final long uncompressedSize;
        private boolean exhausted;

        InnerEventIterator(EventDeserializer eventDeserializer, ByteArrayInputStream inputStream,
                long uncompressedSize) {
            this.eventDeserializer = eventDeserializer;
            this.inputStream = inputStream;
            this.uncompressedSize = uncompressedSize;
        }

        /**
         * @return the next inner event, or {@code null} once the transaction is exhausted (which
         * also closes the cursor).
         * @throws IOException on a decompression or inner-event parse failure
         */
        public Event next() throws IOException {
            if (inputStream == null) {
                return null;
            }
            if (inputStream.getLongPosition() == uncompressedSize) {
                exhausted = true;
                close();
                return null;
            }
            Event event = eventDeserializer.nextEvent(inputStream);
            if (event == null) {
                exhausted = true;
                close();
            } else {
                long position = inputStream.getLongPosition();
                if (position > uncompressedSize) {
                    IOException failure = new IOException("Inner events consumed " + position +
                        " bytes, exceeding transaction payload uncompressed size " + uncompressedSize);
                    try {
                        close();
                    } catch (IOException closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                    throw failure;
                }
                exhausted = position == uncompressedSize;
            }
            return event;
        }

        boolean isExhausted() {
            return exhausted || inputStream == null;
        }

        @Override
        public void close() throws IOException {
            if (inputStream != null) {
                try {
                    // Closes the underlying (zstd) stream, releasing its native decompression context.
                    inputStream.close();
                } finally {
                    inputStream = null;
                    eventDeserializer = null;
                }
            }
        }
    }
}
