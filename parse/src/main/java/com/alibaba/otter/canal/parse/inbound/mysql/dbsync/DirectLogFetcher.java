package com.alibaba.otter.canal.parse.inbound.mysql.dbsync;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tddl.dbsync.binlog.LogFetcher;

/* 基于socket的logEvent实现, 知道整个binlog数据包层面 */
public class DirectLogFetcher extends LogFetcher {
    protected static final Logger logger = LoggerFactory.getLogger(DirectLogFetcher.class);

    public static final byte COM_BINLOG_DUMP = 18;  /* Command to dump binlog */
    public static final int NET_HEADER_SIZE = 4;    /* Packet header sizes */
    public static final int SQLSTATE_LENGTH = 5;    /* sql状态string固定长度 */
    public static final int PACKET_LEN_OFFSET = 0;  /* Packet offsets */
    public static final int PACKET_SEQ_OFFSET = 3;  /* Packet seq */
    public static final int MAX_PACKET_LENGTH = (256 * 256 * 256 - 1);  /* Maximum packet length */

    private SocketChannel channel;

    public DirectLogFetcher() { super(DEFAULT_INITIAL_CAPACITY, DEFAULT_GROWTH_FACTOR); }
    public DirectLogFetcher(final int initialCapacity) { super(initialCapacity, DEFAULT_GROWTH_FACTOR); }
    public DirectLogFetcher(final int initialCapacity, final float growthFactor) { super(initialCapacity, growthFactor); }
    public void start(SocketChannel channel) throws IOException { this.channel = channel; }

    // 获得一个完整的binlog数据包
    public boolean fetch() throws IOException {
        try {
            // Fetching packet header from input.
            if (!fetch0(0, NET_HEADER_SIZE)) {
                logger.warn("Reached end of input stream while fetching header");
                return false;
            }

            // Fetching the first packet(may a multi-packet).
            int netlen = getUint24(PACKET_LEN_OFFSET);
            int netnum = getUint8(PACKET_SEQ_OFFSET);
            if (!fetch0(NET_HEADER_SIZE, netlen)) {
                logger.warn("Reached end of input stream: packet #" + netnum + ", len = " + netlen);
                return false;
            }

            // Detecting error code.
            final int mark = getUint8(NET_HEADER_SIZE);
            if (mark != 0) {
                // 异常情况
                if (mark == 255) // error from master
                {
                    // Indicates an error, for example trying to fetch from
                    // wrong
                    // binlog position.
                    position = NET_HEADER_SIZE + 1;
                    final int errno = getInt16();
                    String sqlstate = forward(1).getFixString(SQLSTATE_LENGTH);
                    String errmsg = getFixString(limit - position);
                    throw new IOException("Received error packet:" + " errno = " + errno + ", sqlstate = " + sqlstate
                            + " errmsg = " + errmsg);
                } else if (mark == 254) {
                    // Indicates end of stream. It's not clear when this would
                    // be sent.
                    logger.warn("Received EOF packet from server, apparent"
                            + " master disconnected. It's may be duplicate slaveId , check instance config");
                    return false;
                } else {
                    // Should not happen.
                    throw new IOException("Unexpected response " + mark + " while fetching binlog: packet #" + netnum
                            + ", len = " + netlen);
                }
            }

            // The first packet is a multi-packet, concatenate the packets.
            while (netlen == MAX_PACKET_LENGTH) {
                if (!fetch0(0, NET_HEADER_SIZE)) {
                    logger.warn("Reached end of input stream while fetching header");
                    return false;
                }

                netlen = getUint24(PACKET_LEN_OFFSET);
                netnum = getUint8(PACKET_SEQ_OFFSET);
                if (!fetch0(limit, netlen)) {
                    logger.warn("Reached end of input stream: packet #" + netnum + ", len = " + netlen);
                    return false;
                }
            }

            // Preparing buffer variables to decoding.
            origin = NET_HEADER_SIZE + 1;
            position = origin;
            limit -= origin;
            return true;
        } catch (SocketTimeoutException e) {
            close(); /* Do cleanup */
            logger.error("Socket timeout expired, closing connection", e);
            throw e;
        } catch (InterruptedIOException e) {
            close(); /* Do cleanup */
            logger.info("I/O interrupted while reading from client socket", e);
            throw e;
        } catch (ClosedByInterruptException e) {
            close(); /* Do cleanup */
            logger.info("I/O interrupted while reading from client socket", e);
            throw e;
        } catch (IOException e) {
            close(); /* Do cleanup */
            logger.error("I/O error while reading from client socket", e);
            throw e;
        }
    }

    // 从socket读取数据到buffer中
    private final boolean fetch0(final int off, final int len) throws IOException {
        ensureCapacity(off + len);

        ByteBuffer buffer = ByteBuffer.wrap(this.buffer, off, len);
        while (buffer.hasRemaining()) {
            int readNum = channel.read(buffer);
            if (readNum == -1) {
                throw new IOException("Unexpected End Stream");
            }
        }

        if (limit < off + len) limit = off + len;
        return true;
    }

    public void close() throws IOException { }
}
