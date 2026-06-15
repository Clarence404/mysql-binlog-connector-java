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
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:somesh.malviya@booking.com">Somesh Malviya</a>
 */
public class TransactionPayloadEventDataDeserializerTest {

      /* DATA is a binary representation of following:
       TransactionPayloadEventData{compression_type=0, payload_size=451, uncompressed_size='960', payload:
           Event{header=EventHeaderV4{timestamp=1646406641000, eventType=QUERY, serverId=223344, headerLength=19, dataLength=57, nextPosition=0, flags=8}, data=QueryEventData{threadId=12, executionTime=0, errorCode=0, database='', sql='BEGIN'}}
           Event{header=EventHeaderV4{timestamp=1646406641000, eventType=TABLE_MAP, serverId=223344, headerLength=19, dataLength=63, nextPosition=0, flags=0}, data=TableMapEventData{tableId=84, database='demo', table='movies', columnTypes=3, 15, 3, 15, 15, 15, 15, 15, 15, 15, 15, columnMetadata=0, 1024, 0, 1024, 1024, 4096, 2048, 1024, 1024, 1024, 1024, columnNullability={}, eventMetadata=TableMapEventMetadata{signedness={}, defaultCharset=255, charsetCollations=null, columnCharsets=null, columnNames=null, setStrValues=null, enumStrValues=null, geometryTypes=null, simplePrimaryKeys=null, primaryKeysWithPrefix=null, enumAndSetDefaultCharset=null, enumAndSetColumnCharsets=null,visibility=null}}}
           Event{header=EventHeaderV4{timestamp=1646406641000, eventType=EXT_UPDATE_ROWS, serverId=223344, headerLength=19, dataLength=756, nextPosition=0, flags=0}, data=UpdateRowsEventData{tableId=84, includedColumnsBeforeUpdate={0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, includedColumns={0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, rows=[
               {before=[1, Once Upon a Time in the West, 1968, Italy, Western, Claudia Cardinale|Charles Bronson|Henry Fonda|Gabriele Ferzetti|Frank Wolff|Al Mulock|Jason Robards|Woody Strode|Jack Elam|Lionel Stander|Paolo Stoppa|Keenan Wynn|Aldo Sambrell, Sergio Leone, Ennio Morricone, Sergio Leone|Sergio Donati|Dario Argento|Bernardo Bertolucci, Tonino Delli Colli, Paramount Pictures], after=[1, Once Upon a Time in the West, 1968, Italy, Western|Action, Claudia Cardinale|Charles Bronson|Henry Fonda|Gabriele Ferzetti|Frank Wolff|Al Mulock|Jason Robards|Woody Strode|Jack Elam|Lionel Stander|Paolo Stoppa|Keenan Wynn|Aldo Sambrell, Sergio Leone, Ennio Morricone, Sergio Leone|Sergio Donati|Dario Argento|Bernardo Bertolucci, Tonino Delli Colli, Paramount Pictures]}
           ]}}
           Event{header=EventHeaderV4{timestamp=1646406641000, eventType=XID, serverId=223344, headerLength=19, dataLength=8, nextPosition=0, flags=0}, data=XidEventData{xid=31}}
           }
      */
      private static final byte[] DATA = {
        2, 1, 0, 3, 3, -4, -64, 3, 1, 3, -4, -61, 1, 0, 40, -75, 47, -3, 0, 88, -68, 13, 0, -90, -34,
        97, 57, 96, 103, -108, 14, 32, 1, 32, 8, -126, 32, 120, 18, 103, 8, -126, -114, 45, -84, -15,
        -9, -66, 68, 74, -118, -40, 82, 68, -110, 16, 13, -122, 26, 35, 98, 20, 123, 16, 7, -5, -10, 69,
        -128, 37, 107, 91, -42, 50, -10, -116, -6, -79, 51, 11, 93, -14, 73, 10, 87, 0, 81, 0, 81, 0,
        -1, -95, 63, -53, -78, 76, -31, -116, -56, -15, -88, -70, 26, 36, -55, -28, -13, 44, 66, -60,
        56, 4, -3, 113, -122, -58, 35, -112, 8, 18, 41, 28, -37, -42, -96, -83, -124, -73, -75, 84, -29,
        -48, 41, 62, -15, -88, -70, 6, 72, -110, -55, 71, -63, -125, 3, -90, -14, 103, -111, 67, 1, -98,
        -3, -15, 71, -125, -126, 88, -108, -16, -1, -104, 7, 79, -24, 6, -66, -16, -57, 53, -113, -86,
        -117, 33, 73, 38, -97, -100, -68, 96, 125, -103, -40, 32, 92, 7, 111, 51, -71, 110, -37, -109,
        -44, 33, 42, -59, -99, 73, -49, -29, 69, 16, -71, 49, -18, 87, 73, 108, -35, -45, -54, 18, -41,
        41, 55, -22, -87, 37, -75, 81, 29, 117, -106, 67, -32, -73, 16, 91, -50, 29, 30, -89, -16, -31,
        0, 126, 7, 4, -120, 45, 39, -73, -126, -55, 45, -41, 106, 20, -87, -55, 125, 49, -56, -99, 120,
        -63, 11, 4, -116, 57, 100, -71, 87, -109, -35, 44, -34, 110, -66, -32, -36, 62, -55, -46, 77,
        54, -27, 40, -111, -39, -61, 73, 86, -34, 77, 16, -11, -70, 26, 110, -78, 93, -85, 68, 124, 75,
        -79, -62, 77, -70, -27, 110, -102, 104, -87, -61, -28, -59, -92, 16, 113, -87, 126, 112, -109,
        30, -86, -101, 19, 49, -22, -87, -44, 19, -55, -115, 41, 68, -68, -104, -38, 117, 34, -46, 81,
        98, 69, -123, -21, -1, -1, -65, -31, 4, 30, 85, 23, -125, 36, -103, 124, -70, -63, -119, 18, 5,
        96, -5, 58, 112, 106, 18, 9, -71, -45, -106, 62, -107, 120, -92, 57, -41, -106, 108, -50, -19,
        37, -101, 27, 55, -59, 35, 109, -102, 58, -82, -31, -37, 74, 54, -11, -108, -33, 86, 98, 67, 94,
        -117, 71, -55, 110, 79, 47, -79, 65, -27, -66, -60, 3, -53, 61, -75, -9, 58, 34, -69, 113, 18,
        0, 9, -123, 64, 53, 121, 75, 21, -68, 7, 33, -73, -30, -127, -103, 9, 17, 66, -49, 84, 65, 2,
        43, 16, -125, 0, 43, 55, 114, 109, 4, -50, -64, -62, -64, 99, 0, 28, -96, 53, -96, -13, 0, -68,
        1, 0, 0
      };

    // Compression type for Zstd is 0
    private static final int COMPRESSION_TYPE = 0;
    private static final int PAYLOAD_SIZE = 451;
    private static final int UNCOMPRESSED_SIZE = 960;
    private static final int NUMBER_OF_UNCOMPRESSED_EVENTS = 4;
    private static final int XID_EVENT_TYPE_CODE = 16;
    private static final String UNCOMPRESSED_UPDATE_EVENT =
      new StringBuilder()
          .append(
              "UpdateRowsEventData{tableId=84, includedColumnsBeforeUpdate={0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, includedColumns={0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, rows=[\n")
          .append(
              "    {before=[1, Once Upon a Time in the West, 1968, Italy, Western, Claudia Cardinale|Charles Bronson|Henry Fonda|Gabriele Ferzetti|Frank Wolff|Al Mulock|Jason Robards|Woody Strode|Jack Elam|Lionel Stander|Paolo Stoppa|Keenan Wynn|Aldo Sambrell, Sergio Leone, Ennio Morricone, Sergio Leone|Sergio Donati|Dario Argento|Bernardo Bertolucci, Tonino Delli Colli, Paramount Pictures],")
          .append(
              " after=[1, Once Upon a Time in the West, 1968, Italy, Western|Action, Claudia Cardinale|Charles Bronson|Henry Fonda|Gabriele Ferzetti|Frank Wolff|Al Mulock|Jason Robards|Woody Strode|Jack Elam|Lionel Stander|Paolo Stoppa|Keenan Wynn|Aldo Sambrell, Sergio Leone, Ennio Morricone, Sergio Leone|Sergio Donati|Dario Argento|Bernardo Bertolucci, Tonino Delli Colli, Paramount Pictures]}\n")
          .append("]}")
          .toString();

    @Test
    public void deserialize() throws IOException {
        TransactionPayloadEventDataDeserializer deserializer = new TransactionPayloadEventDataDeserializer();
        TransactionPayloadEventData transactionPayloadEventData =
            deserializer.deserialize(new ByteArrayInputStream(DATA));
          assertEquals(COMPRESSION_TYPE, transactionPayloadEventData.getCompressionType());
          assertEquals(PAYLOAD_SIZE, transactionPayloadEventData.getPayloadSize());
          assertEquals(UNCOMPRESSED_SIZE, transactionPayloadEventData.getUncompressedSize());
          // Inner events are streamed lazily, not materialized on the event data.
          assertTrue(transactionPayloadEventData.getUncompressedEvents().isEmpty());
          List<Event> innerEvents = drain(deserializer.iterator(transactionPayloadEventData));
          assertEquals(NUMBER_OF_UNCOMPRESSED_EVENTS, innerEvents.size());
          assertEquals(EventType.QUERY, innerEvents.get(0).getHeader().getEventType());
          assertEquals(EventType.TABLE_MAP, innerEvents.get(1).getHeader().getEventType());
          assertEquals(EventType.EXT_UPDATE_ROWS, innerEvents.get(2).getHeader().getEventType());
          assertEquals(EventType.XID, innerEvents.get(3).getHeader().getEventType());
          assertEquals(UNCOMPRESSED_UPDATE_EVENT, innerEvents.get(2).getData().toString());
    }

    @Test
    public void deserializeUncompressedPayload() throws IOException {
        byte[] innerEvent = xidEventBytes(123L);
        byte[] body = payloadEventBody(
            TransactionPayloadEventDataDeserializer.COMPRESSION_TYPE_NONE, null, innerEvent);
        TransactionPayloadEventDataDeserializer deserializer = new TransactionPayloadEventDataDeserializer();

        TransactionPayloadEventData transactionPayloadEventData =
            deserializer.deserialize(new ByteArrayInputStream(body));

        assertEquals(TransactionPayloadEventDataDeserializer.COMPRESSION_TYPE_NONE,
            transactionPayloadEventData.getCompressionType());
        assertEquals(innerEvent.length, transactionPayloadEventData.getUncompressedSize());
        List<Event> innerEvents = drain(deserializer.iterator(transactionPayloadEventData));
        assertEquals(1, innerEvents.size());
        assertEquals(123L, ((XidEventData) innerEvents.get(0).getData()).getXid());
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "Unsupported.*")
    public void deserializeUnsupportedCompressionType() throws IOException {
        TransactionPayloadEventDataDeserializer deserializer = new TransactionPayloadEventDataDeserializer();
        deserializer.deserialize(new ByteArrayInputStream(payloadEventBody(42, 27, new byte[] {1, 2, 3})));
    }

    @Test
    public void deserializeAllowsCompressedPayloadSizeGreaterThanJavaArrayLimit() throws IOException {
        long payloadSize = (long) Integer.MAX_VALUE + 5L;
        TransactionPayloadEventDataDeserializer deserializer = new TransactionPayloadEventDataDeserializer();

        TransactionPayloadEventData transactionPayloadEventData =
            deserializer.deserialize(new ByteArrayInputStream(payloadEventBodyHeaderOnly(
                TransactionPayloadEventDataDeserializer.COMPRESSION_TYPE_NONE, null, payloadSize)));

        assertEquals(transactionPayloadEventData.getPayloadSizeLong(), payloadSize);
        assertEquals(transactionPayloadEventData.getUncompressedSize(), payloadSize);
        assertNull(transactionPayloadEventData.getPayload());
        assertNotNull(transactionPayloadEventData.getPayloadInputStream());
    }

    @Test
    public void deserializeAppliesCompatibilityModesToInnerEvents() throws IOException {
        TransactionPayloadEventDataDeserializer deserializer = new TransactionPayloadEventDataDeserializer();
        EventDeserializer outerDeserializer = new EventDeserializer();
        outerDeserializer.setEventDataDeserializer(EventType.TRANSACTION_PAYLOAD, deserializer);
        outerDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);

        TransactionPayloadEventData transactionPayloadEventData =
            deserializer.deserialize(new ByteArrayInputStream(DATA));
        List<Event> innerEvents = drain(deserializer.iterator(transactionPayloadEventData));
        UpdateRowsEventData updateRowsEventData =
            (UpdateRowsEventData) innerEvents.get(2).getData();

        assertTrue(updateRowsEventData.getRows().get(0).getValue()[1] instanceof byte[]);
    }

    @Test
    public void nextEventTransparentlyUnpacksAndRestampsInnerEvents() throws IOException {
        EventDeserializer eventDeserializer = new EventDeserializer();
        // A TRANSACTION_PAYLOAD (next-position 12345) wrapping two XIDs, followed by a standalone XID,
        // so we also prove the stream stays aligned once the payload's inner events have been drained.
        byte[] payloadEvent = transactionPayloadEvent(12345L, xidEventBytes(111L), xidEventBytes(222L));
        ByteArrayInputStream stream = new ByteArrayInputStream(concat(payloadEvent, xidEventBytes(333L)));

        List<Event> events = new ArrayList<Event>();
        Event event;
        while ((event = eventDeserializer.nextEvent(stream)) != null) {
            events.add(event);
        }

        assertEquals(3, events.size());
        assertEquals(EventType.XID, events.get(0).getHeader().getEventType());
        assertEquals(111L, ((XidEventData) events.get(0).getData()).getXid());
        assertEquals(222L, ((XidEventData) events.get(1).getData()).getXid());
        assertEquals(333L, ((XidEventData) events.get(2).getData()).getXid());

        // Inner events are restamped with the outer envelope's coordinates...
        EventHeaderV4 firstInner = (EventHeaderV4) events.get(0).getHeader();
        assertEquals((long) payloadEvent.length, firstInner.getEventLength());
        assertEquals(12345L, firstInner.getNextPosition());
        // ...while the standalone event read after the payload keeps its own (default 27 / 0).
        assertEquals(27L, ((EventHeaderV4) events.get(2).getHeader()).getEventLength());
        assertFalse(eventDeserializer.hasPendingTransactionPayloadEvent());
    }

    @Test
    public void nextEventDoesNotPrefetchNextInnerEvent() throws IOException {
        EventDeserializer eventDeserializer = new EventDeserializer();
        byte[] firstInnerEvent = xidEventBytes(111L);
        byte[] secondInnerEvent = xidEventBytes(222L);
        byte[] payloadEvent = transactionPayloadEvent(12345L, firstInnerEvent, secondInnerEvent);
        ByteArrayInputStream stream = new ByteArrayInputStream(payloadEvent);

        Event firstEvent = eventDeserializer.nextEvent(stream);

        assertEquals(EventType.XID, firstEvent.getHeader().getEventType());
        assertEquals(111L, ((XidEventData) firstEvent.getData()).getXid());
        assertEquals(stream.getLongPosition(), payloadEvent.length - secondInnerEvent.length);
        assertTrue(eventDeserializer.hasPendingTransactionPayloadEvent());

        Event secondEvent = eventDeserializer.nextEvent(stream);

        assertEquals(EventType.XID, secondEvent.getHeader().getEventType());
        assertEquals(222L, ((XidEventData) secondEvent.getData()).getXid());
        assertEquals(stream.getLongPosition(), payloadEvent.length);
        assertFalse(eventDeserializer.hasPendingTransactionPayloadEvent());
    }

    private static List<Event> drain(TransactionPayloadEventDataDeserializer.InnerEventIterator iterator)
            throws IOException {
        List<Event> events = new ArrayList<Event>();
        try {
            Event event;
            while ((event = iterator.next()) != null) {
                events.add(event);
            }
        } finally {
            iterator.close();
        }
        return events;
    }

    private static byte[] xidEventBytes(long xid) {
        ByteBuffer buf = ByteBuffer.allocate(27).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(1000);
        buf.put((byte) XID_EVENT_TYPE_CODE);
        buf.putInt(1);
        buf.putInt(27);
        buf.putInt(0);
        buf.putShort((short) 0);
        buf.putLong(xid);
        return buf.array();
    }

    private static final int TRANSACTION_PAYLOAD_EVENT_TYPE_CODE = 40;

    // The full on-the-wire bytes of a TRANSACTION_PAYLOAD event (19-byte v4 header + OTW payload header
    // + uncompressed inner events). The header's event-length is the full event size, so the returned
    // array's length equals getEventLength() (there is no checksum).
    private static byte[] transactionPayloadEvent(long nextPosition, byte[]... innerEvents) {
        ByteArrayOutputStream inner = new ByteArrayOutputStream();
        for (byte[] innerEvent : innerEvents) {
            inner.write(innerEvent, 0, innerEvent.length);
        }
        byte[] payload = inner.toByteArray();
        byte[] body = payloadEventBody(
            TransactionPayloadEventDataDeserializer.COMPRESSION_TYPE_NONE, payload.length, payload);

        ByteBuffer header = ByteBuffer.allocate(19).order(ByteOrder.LITTLE_ENDIAN);
        header.putInt(1000);                                    // timestamp
        header.put((byte) TRANSACTION_PAYLOAD_EVENT_TYPE_CODE); // event type
        header.putInt(1);                                       // server id
        header.putInt(19 + body.length);                        // event length (header + body)
        header.putInt((int) nextPosition);                      // next position
        header.putShort((short) 0);                             // flags
        return concat(header.array(), body);
    }

    private static byte[] concat(byte[]... arrays) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] array : arrays) {
            out.write(array, 0, array.length);
        }
        return out.toByteArray();
    }

    private static byte[] payloadEventBody(Integer compressionType, Integer uncompressedSize, byte[] payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (compressionType != null) {
            writePacked(out, TransactionPayloadEventDataDeserializer.OTW_PAYLOAD_COMPRESSION_TYPE_FIELD);
            writePacked(out, packedLength(compressionType));
            writePacked(out, compressionType);
        }
        if (uncompressedSize != null) {
            writePacked(out, TransactionPayloadEventDataDeserializer.OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD);
            writePacked(out, packedLength(uncompressedSize));
            writePacked(out, uncompressedSize);
        }
        writePacked(out, TransactionPayloadEventDataDeserializer.OTW_PAYLOAD_SIZE_FIELD);
        writePacked(out, packedLength(payload.length));
        writePacked(out, payload.length);
        out.write(TransactionPayloadEventDataDeserializer.OTW_PAYLOAD_HEADER_END_MARK);
        out.write(payload, 0, payload.length);
        return out.toByteArray();
    }

    private static byte[] payloadEventBodyHeaderOnly(Integer compressionType, Long uncompressedSize,
            long payloadSize) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (compressionType != null) {
            writePacked(out, TransactionPayloadEventDataDeserializer.OTW_PAYLOAD_COMPRESSION_TYPE_FIELD);
            writePacked(out, packedLength(compressionType));
            writePacked(out, compressionType);
        }
        if (uncompressedSize != null) {
            writePacked(out, TransactionPayloadEventDataDeserializer.OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD);
            writePacked(out, packedLength(uncompressedSize));
            writePacked(out, uncompressedSize);
        }
        writePacked(out, TransactionPayloadEventDataDeserializer.OTW_PAYLOAD_SIZE_FIELD);
        writePacked(out, packedLength(payloadSize));
        writePacked(out, payloadSize);
        out.write(TransactionPayloadEventDataDeserializer.OTW_PAYLOAD_HEADER_END_MARK);
        return out.toByteArray();
    }

    private static void writePacked(ByteArrayOutputStream out, int value) {
        writePacked(out, (long) value);
    }

    private static void writePacked(ByteArrayOutputStream out, long value) {
        if (value < 251) {
            out.write((int) value);
        } else if (value <= 0xFFFFL) {
            out.write(0xFC);
            out.write((int) (value & 0xFF));
            out.write((int) ((value >> 8) & 0xFF));
        } else if (value <= 0xFFFFFFL) {
            out.write(0xFD);
            out.write((int) (value & 0xFF));
            out.write((int) ((value >> 8) & 0xFF));
            out.write((int) ((value >> 16) & 0xFF));
        } else {
            out.write(0xFE);
            for (int i = 0; i < 8; i++) {
                out.write((int) ((value >> (8 * i)) & 0xFF));
            }
        }
    }

    private static int packedLength(int value) {
        return packedLength((long) value);
    }

    private static int packedLength(long value) {
        if (value < 251) {
            return 1;
        } else if (value <= 0xFFFFL) {
            return 3;
        } else if (value <= 0xFFFFFFL) {
            return 4;
        }
        return 9;
    }
}
