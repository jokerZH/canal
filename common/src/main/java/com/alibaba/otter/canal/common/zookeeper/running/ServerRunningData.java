package com.alibaba.otter.canal.common.zookeeper.running;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;

/* 拉取binlog的服务端running状态信息 */
public class ServerRunningData implements Serializable {
    private static final long serialVersionUID = 92260481691855281L;

    private Long              cid;                      // TODO 当前拉取数据的客户端
    private String            address;                  // 服务端的ip地址
    private boolean           active           = true;  // 状态

    public ServerRunningData(){ }
    public ServerRunningData(Long cid, String address){
        this.cid = cid;
        this.address = address;
    }

    public Long getCid() {
        return cid;
    }

    public void setCid(Long cid) {
        this.cid = cid;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}
