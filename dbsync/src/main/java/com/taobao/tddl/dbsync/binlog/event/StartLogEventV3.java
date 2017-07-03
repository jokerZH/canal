package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * Start_log_event_v3 is the Start_log_event of binlog format 3 (MySQL 3.23 and
 * 4.x). Format_description_log_event derives from Start_log_event_v3; it is the
 * Start_log_event of binlog format 4 (MySQL 5.0), that is, the event that
 * describes the other events' Common-Header/Post-Header lengths. This event is
 * sent by MySQL 5.0 whenever it starts sending a new binlog if the requested
 * position is >4 (otherwise if ==4 the event will be sent naturally).
 *
 * Bytes         desc
 * -----         ----
 * 2             The binary log format version. This is 4 in MySQL 5.0 and up.
 * 50            The MySQL server's version (example: 5.0.14-debug-log), padded with 0x00 bytes on the right.
 *
 * @see mysql-5.1.60/sql/log_event.cc - Start_log_event_v3
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public class StartLogEventV3 extends LogEvent {
    /**
     * We could have used SERVER_VERSION_LENGTH, but this introduces an obscure
     * dependency - if somebody decided to change SERVER_VERSION_LENGTH this
     * would break the replication protocol
     * serverVersion的存放长度
     */
    public static final int ST_SERVER_VER_LEN    = 50;

    /* start event post-header (for v3 and v4) */
    // binlogVersion的起始偏移
    public static final int ST_BINLOG_VER_OFFSET = 0;
    // serverVersion的起始偏移
    public static final int ST_SERVER_VER_OFFSET = 2;

    protected int           binlogVersion;      // binlog协议版本
    protected String        serverVersion;      // mysql版本

    public StartLogEventV3(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        // 跳过公共header
        buffer.position(descriptionEvent.commonHeaderLen);
        // ST_BINLOG_VER_OFFSET
        binlogVersion = buffer.getUint16();
        // ST_SERVER_VER_OFFSET
        serverVersion = buffer.getFixString(ST_SERVER_VER_LEN);
    }

    public StartLogEventV3(){ super(new LogHeader(START_EVENT_V3)); }
    public final String getServerVersion() { return serverVersion; }
    public final int getBinlogVersion() { return binlogVersion; }
}
