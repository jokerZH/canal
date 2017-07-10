package com.alibaba.otter.canal.parse.driver.mysql.packets;

import java.io.IOException;

public interface IPacket {
    /* byte[] -> 对象 */
    void fromBytes(byte[] data) throws IOException;

    /* 对象 -> byte[] */
    byte[] toBytes() throws IOException;
}
