package com.alibaba.otter.canal.parse.driver.mysql.packets.server;

import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.PacketWithHeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;

/**
 * The sequence of result set packets:
 *   (Result Set Header Packet)  the number of columns
 *   (Field Packets)             column descriptors
 *   (EOF Packet)                marker: end of Field Packets
 *   (Row Data Packets)          row contents
 *   (EOF Packet)                marker: end of Data Packets
 */
public class ResultSetHeaderPacket extends PacketWithHeaderPacket {
    private long columnCount;       /* 字段的个数 */
    private long extra;             /* TODO */

    public void fromBytes(byte[] data) throws IOException {
        int index = 0;
        byte[] colCountBytes = ByteHelper.readBinaryCodedLengthBytes(data, index);
        columnCount = ByteHelper.readLengthCodedBinary(colCountBytes, index);
        index += colCountBytes.length;
        if (index < data.length - 1) {
            extra = ByteHelper.readLengthCodedBinary(data, index);
        }
    }

    public byte[] toBytes() throws IOException { return null; }
    public long getColumnCount() { return columnCount; }
    public void setColumnCount(long columnCount) { this.columnCount = columnCount; }
    public long getExtra() { return extra; }
    public void setExtra(long extra) { this.extra = extra; }
    public String toString() { return "ResultSetHeaderPacket [columnCount=" + columnCount + ", extra=" + extra + "]"; }
}
