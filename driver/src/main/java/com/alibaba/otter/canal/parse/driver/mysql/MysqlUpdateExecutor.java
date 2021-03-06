package com.alibaba.otter.canal.parse.driver.mysql;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.driver.mysql.packets.client.QueryCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ErrorPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.OKPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager;

/* 默认输出的数据编码为UTF-8，如有需要请正确转码 执行mysql等update操作  */
public class MysqlUpdateExecutor {
    private static final Logger logger = LoggerFactory.getLogger(MysqlUpdateExecutor.class);

    private SocketChannel channel;

    public MysqlUpdateExecutor(MysqlConnector connector) throws IOException {
        if (!connector.isConnected()) {
            throw new IOException("should execute connector.connect() first");
        }

        this.channel = connector.getChannel();
    }

    public MysqlUpdateExecutor(SocketChannel ch) { this.channel = ch; }
    public OKPacket update(String updateString) throws IOException {
        QueryCommandPacket cmd = new QueryCommandPacket();
        cmd.setQueryString(updateString);
        byte[] bodyBytes = cmd.toBytes();
        PacketManager.write(channel, bodyBytes);

        logger.debug("read update result...");
        byte[] body = PacketManager.readBytes(channel, PacketManager.readHeader(channel, 4).getPacketBodyLength());
        if (body[0] < 0) {
            ErrorPacket packet = new ErrorPacket();
            packet.fromBytes(body);
            throw new IOException(packet + "\n with command: " + updateString);
        }

        OKPacket packet = new OKPacket();
        packet.fromBytes(body);
        return packet;
    }
}
