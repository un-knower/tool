/**
 * Apr 14, 2014
 */
package com.hiido.hcat.common;

import java.io.File;

/**
 * @author lin
 *
 */
public final class DeleteunabelFile extends File {
    private static final long serialVersionUID = 1L;
    public DeleteunabelFile(File parent, String pathname) {
        super(parent, pathname);
    }
    @Override
    public boolean delete() {
        return false;
    }
}