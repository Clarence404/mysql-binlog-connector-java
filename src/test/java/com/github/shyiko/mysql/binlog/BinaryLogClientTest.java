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
package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.MySqlGtid;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.TransactionPayloadEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.jmx.BinaryLogClientStatistics;
import com.github.shyiko.mysql.binlog.network.SocketFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class BinaryLogClientTest {

    private static final String SERVER_UUID = "24bc7850-2c16-11e6-a073-0242ac110002";

    @Test
    public void testEventListenersManagement() {
        BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "mysql");
        assertTrue(binaryLogClient.getEventListeners().isEmpty());
        TraceEventListener traceEventListener = new TraceEventListener();
        binaryLogClient.registerEventListener(traceEventListener);
        binaryLogClient.registerEventListener(new CountDownEventListener());
        binaryLogClient.registerEventListener(new CapturingEventListener());
        assertEquals(binaryLogClient.getEventListeners().size(), 3);
        binaryLogClient.unregisterEventListener(traceEventListener);
        assertEquals(binaryLogClient.getEventListeners().size(), 2);
        binaryLogClient.unregisterEventListener(CapturingEventListener.class);
        assertEquals(binaryLogClient.getEventListeners().size(), 1);
    }

    @Test
    public void testLifecycleListenersManagement() {
        BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "mysql");
        assertTrue(binaryLogClient.getLifecycleListeners().isEmpty());
        TraceLifecycleListener traceLifecycleListener = new TraceLifecycleListener();
        binaryLogClient.registerLifecycleListener(traceLifecycleListener);
        binaryLogClient.registerLifecycleListener(new BinaryLogClientStatistics());
        binaryLogClient.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
        });
        assertEquals(binaryLogClient.getLifecycleListeners().size(), 3);
        binaryLogClient.unregisterLifecycleListener(traceLifecycleListener);
        assertEquals(binaryLogClient.getLifecycleListeners().size(), 2);
        binaryLogClient.unregisterLifecycleListener(BinaryLogClientStatistics.class);
        assertEquals(binaryLogClient.getLifecycleListeners().size(), 1);
    }

    @Test(expectedExceptions = TimeoutException.class)
    public void testNoConnectionTimeout() throws Exception {
        new BinaryLogClient("_localhost_", 3306, "root", "mysql").connect(0);
    }

    @Test(timeOut = 15000)
    public void testConnectionTimeout() throws Exception {
        final BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 33059, "root", "mysql");
        final CountDownLatch socketBound = new CountDownLatch(1);
        final CountDownLatch binaryLogClientDisconnected = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final ServerSocket serverSocket = new ServerSocket();
                    try {
                        serverSocket.bind(new InetSocketAddress("localhost", 33059));
                        socketBound.countDown();
                        Socket accept = serverSocket.accept();
                        accept.getOutputStream().write(1);
                        accept.getOutputStream().flush();
                        assertTrue(binaryLogClientDisconnected.await(3000, TimeUnit.MILLISECONDS));
                    } finally {
                        serverSocket.close();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        assertTrue(socketBound.await(3000, TimeUnit.MILLISECONDS));
        binaryLogClient.setConnectTimeout(1000);
        try {
            binaryLogClient.connect();
        } catch (IOException e) {
            binaryLogClientDisconnected.countDown();
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullEventDeserializerIsNotAllowed() throws Exception {
        new BinaryLogClient("localhost", 3306, "root", "mysql").setEventDeserializer(null);
    }

    @Test
    public void testTransactionPayloadNotifiesInnerEvents() throws IOException {
        BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "mysql");
        final List<Event> notifiedEvents = new ArrayList<Event>();
        binaryLogClient.registerEventListener(new BinaryLogClient.EventListener() {
            @Override
            public void onEvent(Event event) {
                notifiedEvents.add(event);
            }
        });

        // The deserializer unpacks the TRANSACTION_PAYLOAD into its inner events before they reach the
        // client, so the client sees ordinary events - here two XIDs restamped with the outer envelope's
        // coordinates (event-length and next-position), as getBinlogPosition() relies on.
        byte[] payloadEvent = transactionPayloadEventBytes(12345L, xidEventBytes(111L), xidEventBytes(222L));
        feedThroughDeserializer(binaryLogClient, payloadEvent);

        assertEquals(notifiedEvents.size(), 2);
        assertEquals(notifiedEvents.get(0).getHeader().getEventType(), EventType.XID);
        assertEquals(((XidEventData) notifiedEvents.get(0).getData()).getXid(), 111L);
        assertEquals(((XidEventData) notifiedEvents.get(1).getData()).getXid(), 222L);

        EventHeaderV4 firstHeader = notifiedEvents.get(0).getHeader();
        assertEquals(firstHeader.getEventLength(), (long) payloadEvent.length);
        assertEquals(firstHeader.getNextPosition(), 12345L);
        assertEquals(firstHeader.getPosition(), 12345L - payloadEvent.length);
        assertEquals(binaryLogClient.getBinlogPosition(), 12345L);
    }

    @Test
    public void testGtidSetAdvancesWhenCompressedTransactionCommitsInsidePayload() throws IOException {
        BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "mysql");
        binaryLogClient.setGtidSet(SERVER_UUID + ":1-5");

        binaryLogClient.handleEvent(gtidEvent(6));
        assertEquals(binaryLogClient.getGtidSet(), SERVER_UUID + ":1-5");

        // The commit (XID) lives inside the compressed payload; once unpacked it advances the gtid set
        // through the same path a standalone XID would.
        feedThroughDeserializer(binaryLogClient, transactionPayloadEventBytes(12345L, xidEventBytes(31L)));
        assertEquals(binaryLogClient.getGtidSet(), SERVER_UUID + ":1-6");

        binaryLogClient.handleEvent(gtidEvent(7));
        feedThroughDeserializer(binaryLogClient, transactionPayloadEventBytes(12945L, xidEventBytes(32L)));
        assertEquals(binaryLogClient.getGtidSet(), SERVER_UUID + ":1-7");
    }

    @Test
    public void testPacketPayloadInputStreamReadsAcrossSplitPackets() throws IOException {
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        wire.write(new byte[] {1, 2, 3}); // bytes remaining in the first full packet after the marker
        writeInteger(wire, 2, 3);         // continuation packet length
        wire.write(7);                    // continuation packet sequence
        wire.write(new byte[] {4, 5});

        InputStream payloadInputStream = new BinaryLogClient.PacketPayloadInputStream(
            new ByteArrayInputStream(wire.toByteArray()), 3, true);
        byte[] result = new byte[5];

        assertEquals(payloadInputStream.read(result, 0, 4), 3);
        assertEquals(payloadInputStream.read(result, 3, 2), 2);
        assertEquals(payloadInputStream.read(), -1);
        assertEquals(result, new byte[] {1, 2, 3, 4, 5});
    }

    @Test(timeOut = 15000)
    public void testDisconnectWhileBlockedByFBRead() throws Exception {
        final BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 33061, "root", "mysql");
        final CountDownLatch readAttempted = new CountDownLatch(1);
        binaryLogClient.setSocketFactory(new SocketFactory() {
            @Override
            public Socket createSocket() throws SocketException {
                return new Socket() {

                    @Override
                    public InputStream getInputStream() throws IOException {
                        return new FilterInputStream(super.getInputStream()) {

                            @Override
                            public int read(byte[] b, int off, int len) throws IOException {
                                readAttempted.countDown();
                                return super.read(b, off, len);
                            }
                        };
                    }
                };
            }
        });
        binaryLogClient.setKeepAlive(false);
        final CountDownLatch socketBound = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final ServerSocket serverSocket = new ServerSocket();
                    try {
                        serverSocket.bind(new InetSocketAddress("localhost", 33061));
                        socketBound.countDown();
                        serverSocket.accept(); // accept socket but do NOT send anything
                        assertTrue(readAttempted.await(3000, TimeUnit.MILLISECONDS));
                        Thread thread = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    Thread.yield();
                                    binaryLogClient.disconnect();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
                        thread.start();
                        thread.join();
                    } finally {
                        serverSocket.close();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        assertTrue(socketBound.await(3000, TimeUnit.MILLISECONDS));
        try {
            binaryLogClient.connect();
        } catch (IOException e) {
            assertEquals(readAttempted.getCount(), 0);
            assertTrue(e.getMessage().contains("Failed to connect to MySQL"));
        }
    }

    private Event gtidEvent(long transactionId) {
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.GTID);
        return new Event(header, new GtidEventData(
            MySqlGtid.fromString(SERVER_UUID + ":" + transactionId),
            (byte) 0, 0L, 0L, 0L, 0L, 0L, 0, 0
        ));
    }

    // The on-the-wire bytes of a standalone XID event (19-byte v4 header + 8-byte xid), as they
    // appear inside an (uncompressed) transaction payload. Inner events carry no checksum.
    private static final int XID_EVENT_TYPE_CODE = 16;
    private static final int TRANSACTION_PAYLOAD_EVENT_TYPE_CODE = 40;

    private byte[] xidEventBytes(long xid) {
        ByteBuffer buf = ByteBuffer.allocate(27).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(1000);                       // timestamp
        buf.put((byte) XID_EVENT_TYPE_CODE);    // event type
        buf.putInt(1);                          // server id
        buf.putInt(27);                         // event length
        buf.putInt(0);                          // next position
        buf.putShort((short) 0);                // flags
        buf.putLong(xid);
        return buf.array();
    }

    // The on-the-wire bytes of a TRANSACTION_PAYLOAD event (19-byte v4 header + OTW payload header +
    // an uncompressed body of inner events), as the deserializer reads them off the stream. Building a
    // COMPRESSION_TYPE_NONE payload drives the same unpack path the deserializer uses for real zstd
    // payloads, without pulling in a compressor. The header's event-length is the full event size, so
    // the returned array's length equals getEventLength() (there is no checksum).
    private byte[] transactionPayloadEventBytes(long nextPosition, byte[]... innerEvents) {
        ByteArrayOutputStream inner = new ByteArrayOutputStream();
        for (byte[] innerEvent : innerEvents) {
            inner.write(innerEvent, 0, innerEvent.length);
        }
        byte[] payload = inner.toByteArray();

        ByteArrayOutputStream body = new ByteArrayOutputStream();
        writeOtwField(body, TransactionPayloadEventDataDeserializer.OTW_PAYLOAD_COMPRESSION_TYPE_FIELD,
            TransactionPayloadEventDataDeserializer.COMPRESSION_TYPE_NONE);
        writeOtwField(body, TransactionPayloadEventDataDeserializer.OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD,
            payload.length);
        writeOtwField(body, TransactionPayloadEventDataDeserializer.OTW_PAYLOAD_SIZE_FIELD, payload.length);
        body.write(TransactionPayloadEventDataDeserializer.OTW_PAYLOAD_HEADER_END_MARK);
        body.write(payload, 0, payload.length);
        byte[] bodyBytes = body.toByteArray();

        ByteBuffer header = ByteBuffer.allocate(19).order(ByteOrder.LITTLE_ENDIAN);
        header.putInt(1000);                                        // timestamp
        header.put((byte) TRANSACTION_PAYLOAD_EVENT_TYPE_CODE);     // event type
        header.putInt(1);                                           // server id
        header.putInt(19 + bodyBytes.length);                       // event length (header + body)
        header.putInt((int) nextPosition);                          // next position
        header.putShort((short) 0);                                 // flags

        ByteArrayOutputStream event = new ByteArrayOutputStream();
        event.write(header.array(), 0, header.array().length);
        event.write(bodyBytes, 0, bodyBytes.length);
        return event.toByteArray();
    }

    private static void writeOtwField(ByteArrayOutputStream out, int fieldType, int value) {
        writePacked(out, fieldType);
        writePacked(out, value < 251 ? 1 : 3); // field length, ignored by the deserializer for known fields
        writePacked(out, value);
    }

    private static void writePacked(ByteArrayOutputStream out, int value) {
        if (value < 251) {
            out.write(value);
        } else {
            out.write(0xFC);
            out.write(value & 0xFF);
            out.write((value >> 8) & 0xFF);
        }
    }

    private static void writeInteger(ByteArrayOutputStream out, int value, int length) {
        for (int i = 0; i < length; i++) {
            out.write((value >> (8 * i)) & 0xFF);
        }
    }

    // Drives the bytes of one or more events through a real EventDeserializer (which transparently
    // unpacks any TRANSACTION_PAYLOAD) and hands each resulting event to the client, exactly as the
    // read loop does.
    private static void feedThroughDeserializer(BinaryLogClient client, byte[] eventBytes) throws IOException {
        EventDeserializer deserializer = new EventDeserializer();
        ByteArrayInputStream stream = new ByteArrayInputStream(eventBytes);
        Event event;
        while ((event = deserializer.nextEvent(stream)) != null) {
            client.handleEvent(event);
        }
    }

    /*
    @Test
    public void testDeadlockyCode() throws IOException, InterruptedException {
        final BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "123456");
        binaryLogClient.setHeartbeatInterval(10000);
        binaryLogClient.setKeepAlive(true);
        binaryLogClient.setKeepAliveInterval(2000);

        binaryLogClient.connect();

        Thread.sleep(1000);

        binaryLogClient.disconnect();
    }
    */
}
