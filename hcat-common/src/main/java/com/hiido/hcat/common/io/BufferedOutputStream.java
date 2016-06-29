/**
 * May 23, 2013
 */
package com.hiido.hcat.common.io;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.exec.LogOutputStream;

/**
 * @author lin
 * 
 */
public class BufferedOutputStream extends LogOutputStream {
    private final List<String> lines = new LinkedList<String>();
    private final int maxCount;
    private final Filter f;
    private final Object sync = new Object();
    private final boolean rmOld;
    private long count;

    private long rmcount;

    public BufferedOutputStream(int level, int maxCount, Filter f, boolean rmOld) {
        super(level);
        this.maxCount = maxCount;
        this.f = f;
        this.rmOld = rmOld;
    }

    public BufferedOutputStream(int level, int max, boolean rmOld) {
        this(level, max, null, true);
    }

    public BufferedOutputStream(int level, int max) {
        this(level, max, true);
    }

    public BufferedOutputStream(int max) {
        // 6 is log4j info level
        this(6, max, true);
    }

    public void write(String str) {
        synchronized (sync) {
            processLine(str);
        }
    }

    public void reset() {
        synchronized (sync) {
            lines.clear();
            count = 0;
        }
    }

    public long getRMCount() {
        return rmcount;
    }

    public List<String> getLines(boolean clean) {
        synchronized (sync) {
            List<String> list = new ArrayList<String>(lines);
            if (clean) {
                lines.clear();
            }
            return list;
        }
    }

    public void getLines(StringBuilder sb, boolean clean) {

    }

    @Override
    protected void processLine(String line, int level) {
        if (line == null) {
            return;
        }
        if (f != null && !f.accept(line, level, count++)) {
            return;
        }
        synchronized (sync) {
            if (maxCount > 0 && lines.size() >= maxCount) {
                rmcount++;
                if (rmOld) {
                    lines.remove(0);
                } else {
                    return;
                }
            }
            lines.add(line);
        }

    }

    public interface Filter {
        boolean accept(String line, int level, long count);
    }

}
