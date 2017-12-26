package com.alibaba.otter.canal.client.impl.running;

/* client running状态信息, 保存在zookeeper中 */
public class ClientRunningData {
    private short   clientId;       // client id
    private String  address;        // 自己的ip地址
    private boolean active = true;  // TODO

    public short getClientId() { return clientId; }
    public void setClientId(short clientId) { this.clientId = clientId; }
    public String getAddress() { return address; }
    public void setAddress(String address) { this.address = address; }
    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }
}
