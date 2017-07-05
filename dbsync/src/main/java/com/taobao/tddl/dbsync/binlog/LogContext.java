package com.taobao.tddl.dbsync.binlog;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent;
import com.taobao.tddl.dbsync.binlog.event.TableMapLogEvent;

/* 保存解析上下文信息 */
public final class LogContext {
    private final Map<Long, TableMapLogEvent> mapOfTable = new HashMap<Long, TableMapLogEvent>();
    private FormatDescriptionLogEvent         formatDescription;
    private LogPosition                       logPosition;

    public LogContext(){ this.formatDescription = FormatDescriptionLogEvent.FORMAT_DESCRIPTION_EVENT_5_x; }
    public LogContext(FormatDescriptionLogEvent descriptionEvent){ this.formatDescription = descriptionEvent; }
    public final LogPosition getLogPosition() { return logPosition; }
    public final void setLogPosition(LogPosition logPosition) { this.logPosition = logPosition; }
    public final FormatDescriptionLogEvent getFormatDescription() { return formatDescription; }
    public final void setFormatDescription(FormatDescriptionLogEvent formatDescription) { this.formatDescription = formatDescription; }
    public final void putTable(TableMapLogEvent mapEvent) { mapOfTable.put(Long.valueOf(mapEvent.getTableId()), mapEvent); }
    public final TableMapLogEvent getTable(final long tableId) { return mapOfTable.get(Long.valueOf(tableId)); }
    public final void clearAllTables() { mapOfTable.clear(); }
    public void reset() {
        formatDescription = FormatDescriptionLogEvent.FORMAT_DESCRIPTION_EVENT_5_x;
        mapOfTable.clear();
    }
}
