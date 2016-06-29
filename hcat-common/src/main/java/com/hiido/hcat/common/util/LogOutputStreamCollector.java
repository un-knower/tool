/**
 * Nov 19, 2012
 */
package com.hiido.hcat.common.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.exec.LogOutputStream;

/**
 * @author lin
 * 
 */
public class LogOutputStreamCollector extends LogOutputStream {
    private final List<String> collection = new ArrayList<String>();

    public LogOutputStreamCollector() {
    }

    @Override
    protected void processLine(String line, int level) {
        collection.add(line);
    }

    public String getString() {
        StringBuilder sb = new StringBuilder((collection.size() + 1) * 128);
        for (String str : collection) {
            sb.append(str);
        }
        return sb.toString();
    }

}
