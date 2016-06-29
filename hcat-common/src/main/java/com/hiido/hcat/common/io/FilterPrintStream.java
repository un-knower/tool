/**
 * Apr 15, 2014
 */
package com.hiido.hcat.common.io;

import java.io.OutputStream;
import java.io.PrintStream;

/**
 * @author lin
 *
 */
public class FilterPrintStream extends PrintStream {
    private volatile String filter;

    public FilterPrintStream(OutputStream out) {
        super(out);
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    @Override
    public void println(String x) {
        if (filter == null || !filter.equals(x)) {
            super.println(x);
        }
    }
}
