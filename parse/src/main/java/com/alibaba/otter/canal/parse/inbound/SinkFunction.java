package com.alibaba.otter.canal.parse.inbound;

/* 用于处理要解析的binlog */

public interface SinkFunction<EVENT> {

    public boolean sink(EVENT event);
}
