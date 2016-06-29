/**
 * Apr 27, 2013
 */
package com.hiido.hcat.common.util;

import java.io.PrintStream;

import org.apache.commons.logging.Log;

/**
 * @author lin
 * 
 */
public abstract class PrintProgress implements Progress {
    protected final PrintStream console;
    protected final Log log;
    protected final String name;
    protected final long total;
    protected final boolean warn;

    protected PrintProgress(String name, long total, PrintStream console, Log log, boolean warn) {
        this.name = name;
        this.total = total;
        this.console = console;
        this.log = log;
        this.warn = warn;
    }

    @Override
    public void report(long pos) {
        if (needReport(pos)) {
            long progress = total == 0 ? 100 : pos * 100 / total;
            String msg = String.format("file[%s],[%d %%](%d/%d)", name, progress, pos, total);
            if (console != null) {
                console.println(msg);
            }
            if (log != null) {
                if (warn) {
                    log.warn(msg);
                } else {
                    log.info(msg);
                }
            }
        }

    }

    protected abstract boolean needReport(long pos);

}
