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

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Holds the inner events of a single {@code TRANSACTION_PAYLOAD} that is being unpacked, handing them
 * out one per call so a compressed transaction is streamed through with bounded memory rather than
 * materialized whole (its uncompressed image may exceed the 2GB {@code byte[]} limit or simply not fit
 * in heap).
 *
 * <p>{@link EventDeserializer} keeps one instance and consults {@link #hasPending()} before reading the
 * underlying stream: while a payload is in flight its remaining inner events
 * are emitted through {@link #next()}; only once it is drained does the
 * deserializer read the next event off the stream. {@link #open} starts a
 * payload; the caller must then call {@link #next()} to retrieve the first
 * inner event.
 */
class TransactionPayloadEventBuffer {

    private TransactionPayloadEventDataDeserializer.InnerEventIterator iterator;
    private EventHeader eventHeader;
    private ByteArrayInputStream inputStream;
    private int checksumLength;

    /**
     * @return {@code true} while a payload opened earlier still has inner events (or a deferred unpack
     * failure) to emit.
     */
    boolean hasPending() {
        return iterator != null;
    }

    /**
     * Begins unpacking the {@code TRANSACTION_PAYLOAD} whose body is next on {@code inputStream}.
     * The compressed payload is consumed from {@code inputStream} incrementally as inner events are
     * pulled (see {@link #next()}), so nothing is prefetched; the stream is left positioned at the end
     * of the whole envelope (payload + checksum) only once the payload has been drained. The caller
     * must call {@link #next()} after this method returns to retrieve the first inner event.
     */
    void open(TransactionPayloadEventDataDeserializer deserializer, ByteArrayInputStream inputStream,
            EventHeader eventHeader, int checksumLength) throws IOException {
        TransactionPayloadEventData eventData = deserializeEnvelope(deserializer, inputStream, eventHeader,
            checksumLength);
        this.eventHeader = eventHeader;
        this.inputStream = inputStream;
        this.checksumLength = checksumLength;
        this.iterator = deserializer.iterator(eventData);
    }

    /**
     * @return the next buffered inner event, or {@code null} once the current payload is fully drained
     * (at which point the deserializer resumes reading the underlying stream).
     * @throws IOException re-raising a deferred close failure, or wrapping a decompression /
     * inner-event parse failure.
     */
    Event next() throws IOException {
        if (iterator == null) {
            return null;
        }
        Event innerEvent;
        try {
            innerEvent = iterator.next();
        } catch (IOException | RuntimeException e) {
            // A decompression/parse failure partway through the payload cannot be skipped cleanly the
            // way a standalone undeserializable event can (earlier inner events were already emitted).
            // Surface it as a plain deserialization failure - never an EOFException/SocketException - so
            // the reader reports it and moves on instead of treating it as a transport failure and
            // reconnecting only to refetch and re-fail on the same payload.
            close();
            throw new IOException("Failed to deserialize inner event of TRANSACTION_PAYLOAD", e);
        }

        if (innerEvent == null) {
            close();
            return null;
        }
        stampOuterEventCoordinates(eventHeader, innerEvent);
        if (iterator.isExhausted()) {
            try {
                close();
            } catch (IOException e) { }
        }
        return innerEvent;
    }

    // Parses just the on-the-wire payload header (compression type / sizes), leaving the compressed
    // payload on the stream for the iterator to decompress lazily. On failure the envelope is drained
    // (payload + checksum) so the stream stays aligned for the next event.
    private TransactionPayloadEventData deserializeEnvelope(TransactionPayloadEventDataDeserializer deserializer,
            ByteArrayInputStream inputStream, EventHeader eventHeader, int checksumLength)
            throws EventDataDeserializationException {
        long eventBodyLength = eventHeader.getDataLength() - checksumLength;
        try {
            inputStream.enterBlock(eventBodyLength);
            return deserializer.deserialize(inputStream, false);
        } catch (IOException e) {
            try {
                inputStream.skipToTheEndOfTheBlock();
                inputStream.skip(checksumLength);
            } catch (IOException skipFailure) {
                e.addSuppressed(skipFailure);
            }
            throw new EventDataDeserializationException(eventHeader, e);
        }
    }

    private void close() throws IOException {
        iterator.close();
        iterator = null;
        eventHeader = null;
        finishBlock();
    }

    // Drains any payload bytes the decompressor did not consume, then the checksum, leaving the stream
    // positioned at the start of the next event.
    private void finishBlock() throws IOException {
        if (inputStream != null) {
            try {
                inputStream.skipToTheEndOfTheBlock();
                inputStream.skip(checksumLength);
            } finally {
                inputStream = null;
                checksumLength = 0;
            }
        }
    }

    // Inner events are decoded from the payload's own byte stream, so their headers carry positions
    // relative to that stream. Restamp each one with the outer envelope's length and next-position so a
    // consumer tracking getBinlogPosition() advances to the transaction boundary, as for any event.
    private static void stampOuterEventCoordinates(EventHeader outerHeader, Event innerEvent) {
        EventHeader innerHeader = innerEvent.getHeader();
        if (outerHeader instanceof EventHeaderV4 && innerHeader instanceof EventHeaderV4) {
            EventHeaderV4 outerHeaderV4 = (EventHeaderV4) outerHeader;
            EventHeaderV4 innerHeaderV4 = (EventHeaderV4) innerHeader;
            innerHeaderV4.setEventLength(outerHeaderV4.getEventLength());
            innerHeaderV4.setNextPosition(outerHeaderV4.getNextPosition());
        }
    }
}
