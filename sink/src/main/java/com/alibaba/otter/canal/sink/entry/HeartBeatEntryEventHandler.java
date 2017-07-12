package com.alibaba.otter.canal.sink.entry;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.sink.AbstractCanalEventDownStreamHandler;
import com.alibaba.otter.canal.store.model.Event;

/* 处理heartbeat数据, 摘除HEARTBEAT消息 */
public class HeartBeatEntryEventHandler extends AbstractCanalEventDownStreamHandler<List<Event>> {

    public List<Event> before(List<Event> events) {
        boolean existHeartBeat = false;
        for (Event event : events) {
            if (event.getEntry().getEntryType() == EntryType.HEARTBEAT) {
                existHeartBeat = true;
            }
        }

        if (!existHeartBeat) {
            return events;
        } else {
            // 目前heartbeat和其他事件是分离的，保险一点还是做一下检查处理
            List<Event> result = new ArrayList<Event>();
            for (Event event : events) {
                if (event.getEntry().getEntryType() != EntryType.HEARTBEAT) {
                    result.add(event);
                }
            }

            return result;
        }
    }
}
