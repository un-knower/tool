/**
 * May 16, 2013
 */
package com.hiido.hcat.common.io;

import java.io.FilterOutputStream;
import java.io.OutputStream;

/**
 * @author lin
 * 
 */
public abstract class ChecksumOutputStream<T> extends FilterOutputStream {

    protected ChecksumOutputStream(OutputStream out) {
        super(out);
    }

    public abstract T getChecksum();

}
