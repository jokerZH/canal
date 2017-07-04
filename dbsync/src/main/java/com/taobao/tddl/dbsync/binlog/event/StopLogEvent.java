package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 *
 * A Stop_log_event is written under these circumstances:
 *  A master writes the event to the binary log when it shuts down
 *  A slave writes the event to the relay log when it shuts down or when a RESET SLAVE statement is executed
 *
 * Stop_log_event. The Post-Header and Body for this event type are empty; it
 * only has the Common-Header.
 *
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class StopLogEvent extends LogEvent {

    public StopLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent description_event){
        super(header);
    }
}
