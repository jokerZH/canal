package com.alibaba.otter.canal.parse.index;

import java.util.Map;
import java.util.Set;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.google.common.collect.MapMaker;

/* 内存型的位点 */
public class MemoryLogPositionManager extends AbstractLogPositionManager {
    private Map<String, LogPosition> positions;

    @Override
    public void start() {
        super.start();
        positions = new MapMaker().makeMap();
    }

    @Override
    public void stop() {
        super.stop();
        positions.clear();
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) { return positions.get(destination); }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        positions.put(destination, logPosition);
    }

    public Set<String> destinations() { return positions.keySet(); }
}
