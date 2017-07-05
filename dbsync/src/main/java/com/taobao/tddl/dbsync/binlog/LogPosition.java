package com.taobao.tddl.dbsync.binlog;

public class LogPosition implements Cloneable, Comparable<LogPosition> {
    protected String fileName;  /* binlog file's name */
    protected long   position;  /* position in file */

    public LogPosition(String fileName){
        this.fileName = fileName;
        this.position = 0L;
    }

    public LogPosition(String fileName, final long position){
        this.fileName = fileName;
        this.position = position;
    }

    public LogPosition(LogPosition source){
        this.fileName = source.fileName;
        this.position = source.position;
    }

    public final String getFileName() { return fileName; }
    public final long getPosition() { return position; }


    public LogPosition clone() {
        try {
            return (LogPosition) super.clone();
        } catch (CloneNotSupportedException e) {
            // Never happend
            return null;
        }
    }

    public final int compareTo(String fileName, final long position) {
        final int val = this.fileName.compareTo(fileName);

        if (val == 0) {
            return (int) (this.position - position);
        }
        return val;
    }

    public int compareTo(LogPosition o) {
        final int val = fileName.compareTo(o.fileName);

        if (val == 0) {
            return (int) (position - o.position);
        }
        return val;
    }

    public boolean equals(Object obj) {
        if (obj instanceof LogPosition) {
            LogPosition pos = ((LogPosition) obj);
            return fileName.equals(pos.fileName) && (this.position == pos.position);
        }
        return false;
    }

    public String toString() { return fileName + ':' + position; }
}
