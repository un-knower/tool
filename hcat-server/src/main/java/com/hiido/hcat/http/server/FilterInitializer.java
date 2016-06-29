/**
 * Aug 8, 2012
 */
package com.hiido.hcat.http.server;

import org.apache.hadoop.conf.Configuration;

/**
 * @author lin
 * 
 */
public interface FilterInitializer {
    public abstract void initFilter(FilterContainer container, Configuration conf);
}
