package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * When a binary log file exceeds a size limit, a ROTATE_EVENT is written
 * at the end of the file that points to the next file in the squence.
 * This event is information for the slave to know the name of
 * the next binary log it is going to receive.
 *
 * This will be deprecated when we move to using sequence ids. Binary Format The
 *
 *          Post-Header for Rotate_log_event
 *  bytes           Name
 *  -----           ----
 *  8               position
 *                  The position within the binlog to rotate to
 *
 *
 *          Body for Rotate_log_event
 *  bytes           Name
 *  -----           ----
 * string           new_log Name of the binlog to rotate to
 *                  variable length string without trailing zero, extending to the end of the
 *                  event (determined by the length field of the Common-Header)
 *
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class RotateLogEvent extends LogEvent {

    /**
     * Fixed data part:
     * <ul>
     * <li>8 bytes. The position of the first event in the next log file. Always
     * contains the number 4 (meaning the next event starts at position 4 in the
     * next binary log). This field is not present in v1; presumably the value
     * is assumed to be 4.</li>
     * </ul>
     * <p>
     * Variable data part:
     * <ul>
     * <li>The name of the next binary log. The filename is not null-terminated.
     * Its length is the event size minus the size of the fixed parts.</li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */
    private final String          filename;
    private final long            position;

    /* Rotate event post-header */
    public static final int       R_POS_OFFSET   = 0;
    public static final int       R_IDENT_OFFSET = 8;

    /* Max length of full path-name */
    public static final int       FN_REFLEN      = 512;

    // Rotate header with all empty fields.
    public static final LogHeader ROTATE_HEADER  = new LogHeader(ROTATE_EVENT);

    /* Creates a new <code>Rotate_log_event</code> object read normally from log */
    public RotateLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        final int headerSize = descriptionEvent.commonHeaderLen;
        final int postHeaderLen = descriptionEvent.postHeaderLen[ROTATE_EVENT - 1];

        // 获得偏移
        buffer.position(headerSize + R_POS_OFFSET);
        position = (postHeaderLen != 0) ? buffer.getLong64() : 4; // !uint8korr(buf + R_POS_OFFSET)

        // 获得文件名
        final int filenameOffset = headerSize + postHeaderLen;
        int filenameLen = buffer.limit() - filenameOffset;
        if (filenameLen > FN_REFLEN - 1) filenameLen = FN_REFLEN - 1;
        buffer.position(filenameOffset);

        filename = buffer.getFixString(filenameLen);
    }

    /* Creates a new <code>Rotate_log_event</code> without log information. This is used to generate missing log rotation events. */
    public RotateLogEvent(String filename){
        super(ROTATE_HEADER);

        this.filename = filename;
        this.position = 4;
    }

    /* Creates a new <code>Rotate_log_event</code> without log information. */
    public RotateLogEvent(String filename, final long position){
        super(ROTATE_HEADER);

        this.filename = filename;
        this.position = position;
    }

    public final String getFilename() { return filename; }
    public final long getPosition() { return position; }
}
