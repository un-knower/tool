/**
 * Apr 27, 2013
 */
package com.hiido.hcat.common;

/**
 * @author lin
 * 
 */
public final class NullPrintProgress extends PrintProgress {
    public static final NullPrintProgress nullProgress = new NullPrintProgress();

    private NullPrintProgress() {
        super(null, 0, null, null, false);
    }

    @Override
    public void report(long pos) {
        return;
    }

    @Override
    protected boolean needReport(long pos) {
        return false;
    }

}
