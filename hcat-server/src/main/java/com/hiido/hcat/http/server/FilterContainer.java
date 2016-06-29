/**
 * Aug 8, 2012
 */
package com.hiido.hcat.http.server;

import java.util.Map;

/**
 * @author lin
 * 
 */
public interface FilterContainer {

    void addFilter(String name, String classname, Map<String, String> parameters);

    void addGlobalFilter(String name, String classname, Map<String, String> parameters);
}
