package com.alibaba.otter.canal.parse.inbound;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.protocol.CanalEntry;

/* 解析binlog的接口 */
public interface BinlogParser<T> extends CanalLifeCycle {
    CanalEntry.Entry parse(T event) throws CanalParseException;
    void reset();
}
