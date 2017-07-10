package com.alibaba.otter.canal.parse.inbound.group;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.parse.CanalEventParser;

/* 组合多个EventParser进行合并处理，group只是做为一个delegate处理 */
public class GroupEventParser extends AbstractCanalLifeCycle implements CanalEventParser {
    private List<CanalEventParser> eventParsers = new ArrayList<CanalEventParser>();

    public void start() {
        super.start();
        // 统一启动
        for (CanalEventParser eventParser : eventParsers) {
            if (!eventParser.isStart()) {
                eventParser.start();
            }
        }
    }

    public void stop() {
        super.stop();
        // 统一关闭
        for (CanalEventParser eventParser : eventParsers) {
            if (eventParser.isStart()) {
                eventParser.stop();
            }
        }
    }

    public void addEventParser(CanalEventParser eventParser) {
        if (!eventParsers.contains(eventParser)) {
            eventParsers.add(eventParser);
        }
    }
    public void setEventParsers(List<CanalEventParser> eventParsers) { this.eventParsers = eventParsers; }
    public void removeEventParser(CanalEventParser eventParser) { eventParsers.remove(eventParser); }
    public List<CanalEventParser> getEventParsers() { return eventParsers; }
}
