package com.hiido.hcat.common.util;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class OrderProperties extends Properties {
    private static final long serialVersionUID = 1508089476293616377L;

    private final List<Object> keys = new ArrayList<Object>();

    public OrderProperties putProperties(OrderProperties oprops) {
        Enumeration<?> en = oprops.propertyNames();
        while (en.hasMoreElements()) {
            Object key = en.nextElement();
            put(key, oprops.get(key));
        }
        return this;
    }

    @Override
    public synchronized Object put(Object key, Object value) {
        if (!containsKey(key)) {
            keys.add(key);
        }
        return super.put(key, value);
    }

    @Override
    public Enumeration<?> propertyNames() {
        return new KeysEnumeration();
    }

    @Override
    public synchronized void clear() {
        keys.clear();
        super.clear();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        Enumeration<?> en = this.propertyNames();
        builder.append("{");
        boolean notempty = false;
        while (en.hasMoreElements()) {
            Object key = en.nextElement();
            Object val = get(key);
            builder.append(key).append("=").append(val);
            if (!notempty) {
                notempty = true;
            }
        }
        if (notempty) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    private final class KeysEnumeration implements Enumeration<Object> {
        private final Iterator<Object> it;

        KeysEnumeration() {
            List<Object> cloneKeys = new ArrayList<Object>(keys);
            it = cloneKeys.iterator();
        }

        @Override
        public boolean hasMoreElements() {
            return it.hasNext();
        }

        @Override
        public Object nextElement() {
            return it.next();
        }

    }
}
