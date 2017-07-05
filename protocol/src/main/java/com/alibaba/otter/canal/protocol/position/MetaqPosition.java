package com.alibaba.otter.canal.protocol.position;

/* TODO */
public class MetaqPosition extends Position {
    private static final long serialVersionUID = -8673508769040569273L;

    private String topic;
    private String msgNewId;
    private long offset;

    public MetaqPosition(String topic, String msgNewId, long offset) {
        super();
        this.topic = topic;
        this.msgNewId = msgNewId;
        this.offset = offset;
    }

    public String getTopic() { return topic; }
    public String getMsgNewId() { return msgNewId; }
    public void setTopic(String topic) { this.topic = topic; }
    public void setMsgNewId(String msgNewId) { this.msgNewId = msgNewId; }
    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }
}
