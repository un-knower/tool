/**
 * Apr 27, 2013
 */
package com.hiido.hcat.common;

import java.io.PrintStream;

import org.apache.commons.logging.Log;

import com.hiido.hcat.common.util.SystemUtils;

public final class TimePrintProgress extends PrintProgress {
    private long lastPrintTime;

    private long printInterval = 1000 * 60 * 3;

    public long getPrintInterval() {
        return printInterval;
    }

    public void setPrintInterval(long printInterval) {
        this.printInterval = printInterval;
    }

    public TimePrintProgress(String name, long total, PrintStream console, Log log, boolean warn) {
        super(name, total, console, log, warn);
    }

    @Override
    protected boolean needReport(long pos) {
        if (pos == total) {
            return true;
        }
        long now = SystemUtils.vagueTime();
        if (now - lastPrintTime > printInterval) {
            lastPrintTime = now;
            return true;
        }

        return false;
    }

}
