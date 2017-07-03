package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * The Common-Header, documented in the table @ref Table_common_header "below",
 * always has the same form and length within one version of MySQL. Each event
 * type specifies a format and length of the Post-Header. The length of the
 * Common-Header is the same for all events of the same type. The Body may be of
 * different format and length even for different events of the same type. The
 * binary formats of Post-Header and Body are documented separately in each
 * subclass. The binary format of Common-Header is as follows.
 *
 * Common-Header
 *
 * Bytes            type        desc
 * -----            ----        ----
 * 4                timestamp   The time when the query started, in seconds since 1970
 * 1                byte        Log_event_type
 * 4                unsigned    server id
 * 4                unsigned    The total size of this event, in bytes. Common-Header, Post-Header, and Body
 * 4                unsigned    master_position. The position of the next event in the master binary log, in bytes from
 *                              the beginning of the file. In a binlog that is not a relay log, this is just
 *                              the position of the next event, in bytes from the beginning of the file. In a
 *                              relay log, this is the position of the next event in the master's binlog.
 * 2                bit field   flags See Log_event::flags.
 *
 * Summing up the numbers above, we see that the total size of the common header is 19 bytes.
 * 
 * @see mysql-5.1.60/sql/log_event.cc
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class LogHeader {
    /* The different types of log events. */
    protected final int type;

    /**
     * The offset in the log where this event originally appeared (it is
     * preserved in relay logs, making SHOW SLAVE STATUS able to print
     * coordinates of the event in the master's binlog). Note: when a
     * transaction is written by the master to its binlog (wrapped in
     * BEGIN/COMMIT) the log_pos of all the queries it contains is the one of
     * the BEGIN (this way, when one does SHOW SLAVE STATUS it sees the offset
     * of the BEGIN, which is logical as rollback may occur), except the COMMIT
     * query which has its real offset.
     */
    protected long      logPos;

    /**
     * Timestamp on the master(for debugging and replication of
     * NOW()/TIMESTAMP). It is important for queries and LOAD DATA INFILE. This
     * is set at the event's creation time, except for Query and Load (et al.)
     * events where this is set at the query's execution time, which guarantees
     * good replication (otherwise, we could have a query and its event with
     * different timestamps).
     */
    protected long      when;

    /** Number of bytes written by write() function */
    protected int       eventLen;

    /**
     * The master's server id (is preserved in the relay log; used to prevent
     * from infinite loops in circular replication).
     */
    protected long      serverId;

    /**
     * Some 16 flags. See the definitions above for LOG_EVENT_TIME_F,
     * LOG_EVENT_FORCED_ROTATE_F, LOG_EVENT_THREAD_SPECIFIC_F, and
     * LOG_EVENT_SUPPRESS_USE_F for notes.
     */
    protected int       flags;

    /**
     * The value is set by caller of FD constructor and
     * Log_event::write_header() for the rest. In the FD case it's propagated
     * into the last byte of post_header_len[] at FD::write(). On the slave side
     * the value is assigned from post_header_len[last] of the last seen FD
     * event.
     */
    protected int       checksumAlg;

    /**
     * Placeholder for event checksum while writing to binlog.
     */
    protected long      crc;        // ha_checksum

    /* for Start_event_v3 */
    public LogHeader(final int type){ this.type = type; }
    /* 读取FormatDescriptionLogEvent数据包 */
    public LogHeader(LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        // logEvent.TIME_STAMP_OFFSET
        when = buffer.getUint32();

        // LogEvent.EVENT_TYPE_OFFSET;
        type = buffer.getUint8();

        // LogEvent.SERVER_ID_OFFSET;
        serverId = buffer.getUint32();

        // LogEvent.EVENT_LEN_OFFSET;
        eventLen = (int) buffer.getUint32();

        if (descriptionEvent.binlogVersion == 1) {
            logPos = 0;
            flags = 0;
            return;
        }

        // LogEvent.LOG_POS_OFFSET for 4.0 or newer
        logPos = buffer.getUint32();
        /*
         * If the log is 4.0 (so here it can only be a 4.0 relay log read by the
         * SQL thread or a 4.0 master binlog read by the I/O thread), log_pos is
         * the beginning of the event: we transform it into the end of the
         * event, which is more useful. But how do you know that the log is 4.0:
         * you know it if description_event is version 3 *and* you are not
         * reading a Format_desc (remember that mysqlbinlog starts by assuming
         * that 5.0 logs are in 4.0 format, until it finds a Format_desc).
         * TODO 3以前记录的pos是当前binlog的偏移,而4记录的是下一个binlog的偏移，这里统一下,
         * TODO 而logPos＝＝0的情况是特殊情况采用的，如rotate文件的时候
         */
        if (descriptionEvent.binlogVersion == 3 && type < LogEvent.FORMAT_DESCRIPTION_EVENT && logPos != 0) {
            /*
             * If log_pos=0, don't change it. log_pos==0 is a marker to mean
             * "don't change rli->group_master_log_pos" (see
             * inc_group_relay_log_pos()). As it is unreal log_pos, adding the
             * event len's is nonsense. For example, a fake Rotate event should
             * not have its log_pos (which is 0) changed or it will modify
             * Exec_master_log_pos in SHOW SLAVE STATUS, displaying a nonsense
             * value of (a non-zero offset which does not exist in the master's
             * binlog, so which will cause problems if the user uses this value
             * in CHANGE MASTER).
             */
            logPos += eventLen; /* purecov: inspected */
        }

        // LogEvent.FLAGS_OFFSET
        flags = buffer.getUint16();
        if ((type == LogEvent.FORMAT_DESCRIPTION_EVENT) || (type == LogEvent.ROTATE_EVENT)) {
            /*
             * These events always have a header which stops here (i.e. their
             * header is FROZEN).
             */
            /*
             * Initialization to zero of all other Log_event members as they're
             * not specified. Currently there are no such members; in the future
             * there will be an event UID (but Format_description and Rotate
             * don't need this UID, as they are not propagated through
             * --log-slave-updates (remember the UID is used to not play a query
             * twice when you have two masters which are slaves of a 3rd
             * master). Then we are done.
             */

            if (type == LogEvent.FORMAT_DESCRIPTION_EVENT) {
                int commonHeaderLen = buffer.getUint8(FormatDescriptionLogEvent.LOG_EVENT_MINIMAL_HEADER_LEN
                                                      + FormatDescriptionLogEvent.ST_COMMON_HEADER_LEN_OFFSET);
                buffer.position(commonHeaderLen + FormatDescriptionLogEvent.ST_SERVER_VER_OFFSET);
                String serverVersion = buffer.getFixString(FormatDescriptionLogEvent.ST_SERVER_VER_LEN); // ST_SERVER_VER_OFFSET
                int versionSplit[] = new int[] { 0, 0, 0 };
                FormatDescriptionLogEvent.doServerVersionSplit(serverVersion, versionSplit);
                checksumAlg = LogEvent.BINLOG_CHECKSUM_ALG_UNDEF;
                if (FormatDescriptionLogEvent.versionProduct(versionSplit) >= FormatDescriptionLogEvent.checksumVersionProduct) {
                    buffer.position(eventLen - LogEvent.BINLOG_CHECKSUM_LEN - LogEvent.BINLOG_CHECKSUM_ALG_DESC_LEN);
                    checksumAlg = buffer.getUint8();
                }

                processCheckSum(buffer);
            }
            return;
        }

        /*
         * CRC verification by SQL and Show-Binlog-Events master side. The
         * caller has to provide @description_event->checksum_alg to be the last
         * seen FD's (A) descriptor. If event is FD the descriptor is in it.
         * Notice, FD of the binlog can be only in one instance and therefore
         * Show-Binlog-Events executing master side thread needs just to know
         * the only FD's (A) value - whereas RL can contain more. In the RL
         * case, the alg is kept in FD_e (@description_event) which is reset to
         * the newer read-out event after its execution with possibly new alg
         * descriptor. Therefore in a typical sequence of RL: {FD_s^0, FD_m,
         * E_m^1} E_m^1 will be verified with (A) of FD_m. See legends
         * definition on MYSQL_BIN_LOG::relay_log_checksum_alg docs lines
         * (log.h). Notice, a pre-checksum FD version forces alg :=
         * BINLOG_CHECKSUM_ALG_UNDEF.
         */
        checksumAlg = descriptionEvent.getHeader().checksumAlg; // fetch
                                                                // checksum alg
        processCheckSum(buffer);
        /* otherwise, go on with reading the header from buf (nothing now) */
    }

    public final int getType() { return type; }
    public final long getLogPos() { return logPos; }
    public final int getEventLen() { return eventLen; }
    public final long getWhen() { return when; }
    public final long getServerId() { return serverId; }
    public final int getFlags() { return flags; }
    public long getCrc() { return crc; }
    public int getChecksumAlg() { return checksumAlg; }
    private void processCheckSum(LogBuffer buffer) {
        if (checksumAlg != LogEvent.BINLOG_CHECKSUM_ALG_OFF &&
                checksumAlg != LogEvent.BINLOG_CHECKSUM_ALG_UNDEF) {
            crc = buffer.getUint32(eventLen - LogEvent.BINLOG_CHECKSUM_LEN);
        }
    }
}
