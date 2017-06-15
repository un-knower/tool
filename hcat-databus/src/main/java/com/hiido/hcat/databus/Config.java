package com.hiido.hcat.databus;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zrc on 16-11-30.
 */
public class Config {
    public static final String HCAT_DATABUS_SERVICE_TYPE_KEY = "hcat.databus.service.type.key";
    public static final String HCAT_DATABUS_SERVICE_ADDRESS = "hcat.databus.service.address";
    public static final String HCAT_DATABUS_DATA_STORETIME_COLUMN = "hcat.databus.data.storetime.column";
    public static final String HCAT_DATABUS_RESET_DATA = "hcat.databus.reset.data";
    public static final String HCAT_DATABUS_PUSHALL_ONCE_REQUEST = "hcat.databus.pushall.once.request";


    private Map<String, String> confData = new HashMap<String, String>();

    public static enum Vars {

        RPCSERVER_ADDRESS("hcat.databus.rpc.server.address", "0.0.0.0"),
        PRCSERVER_PORT("hcat.databus.rpc.server.port", 0),
        RPCSERVER_BOSS_THREADCOUNT("hcat.databus.rpc.boss.threadCount", 8),
        RPCSERVER_WORK_THREADCOUNT("hcat.databus.rpc.work.threadCount", 8);

        private final String varname;

        public final String defaultStrVal;
        public final int defaultIntVal;
        public final long defaultLongVal;
        public final float defaultFloatVal;
        public final boolean defaultBoolVal;
        private final Class<?> varClass;

        Vars(String varname, String defaultValue) {
            this.varname = varname;
            this.defaultStrVal = defaultValue;
            this.varClass = String.class;
            this.defaultIntVal = -1;
            this.defaultLongVal = -1;
            this.defaultFloatVal = -1;
            this.defaultBoolVal = false;
        }

        Vars(String varname, int defaultValue) {
            this.varname = varname;
            this.defaultIntVal = defaultValue;
            this.varClass = Integer.class;
            this.defaultStrVal = null;
            this.defaultLongVal = -1;
            this.defaultFloatVal = -1;
            this.defaultBoolVal = false;
        }

        Vars(String varname, long defaultValue) {
            this.varname = varname;
            this.defaultIntVal = -1;
            this.defaultStrVal = null;
            this.defaultLongVal = defaultValue;
            this.varClass = Long.class;
            this.defaultFloatVal = -1;
            this.defaultBoolVal = false;
        }

        Vars(String varname, float defaultValue) {
            this.varname = varname;
            this.defaultIntVal = -1;
            this.defaultStrVal = null;
            this.defaultLongVal = -1;
            this.defaultFloatVal = defaultValue;
            this.varClass = Float.class;
            this.defaultBoolVal = false;
        }

        Vars(String varname, boolean defaultValue) {
            this.varname = varname;
            this.defaultIntVal = -1;
            this.defaultStrVal = null;
            this.defaultLongVal = -1;
            this.defaultFloatVal = -1;
            this.defaultBoolVal = defaultValue;
            this.varClass = Boolean.class;
        }
    }

    enum VarType {
        STRING {
            @Override
            void checkType(String value) throws Exception { }
        },
        INT {
            @Override
            void checkType(String value) throws Exception { Integer.valueOf(value); }
        },
        LONG {
            @Override
            void checkType(String value) throws Exception { Long.valueOf(value); }
        },
        FLOAT {
            @Override
            void checkType(String value) throws Exception { Float.valueOf(value); }
        },
        BOOLEAN {
            @Override
            void checkType(String value) throws Exception { Boolean.valueOf(value); }
        };

        boolean isType(String value) {
            try { checkType(value); } catch (Exception e) { return false; }
            return true;
        }
        String typeString() { return name().toUpperCase();}
        abstract void checkType(String value) throws Exception;
    }

    public int getIntVar(Vars var) {
        assert(var.varClass == Integer.class) : var.varname;
        String valueString = confData.get(var.varname);
        if(valueString == null)
            return var.defaultIntVal;
        return Integer.parseInt(valueString);
    }

    public long getLongVar(Vars var) {
        assert(var.varClass == Long.class) : var.varname;
        String valueString = confData.get(var.varname);
        if(valueString == null)
            return var.defaultLongVal;
        return Long.parseLong(valueString);
    }

    public float getFloatVar(Vars var) {
        assert (var.varClass == Float.class) : var.varname;
        String valueString = confData.get(var.varname);
        if(valueString == null)
            return var.defaultFloatVal;
        return Float.parseFloat(valueString);
    }

    public boolean getBoolVar(Vars var) {
        assert (var.varClass == Boolean.class) : var.varname;
        String valueString = confData.get(var.varname);
        if(valueString == null)
            return var.defaultBoolVal;
        return Boolean.parseBoolean(valueString);
    }

    public String getStrVar(Vars var) {
        assert (var.varClass == String.class) : var.varname;
        String valueString = confData.get(var.varname);
        if(valueString == null)
            return var.defaultStrVal;
        return valueString;
    }

    public String get(String varname) {
        return confData.get(varname);
    }

    public int getInt(String varname) {
        return (Integer.parseInt(confData.get(varname)));
    }

    public long getLong(String varname) {
        return (Long.parseLong(confData.get(varname)));
    }

    public float getFloat(String varname) {
        return (Float.parseFloat(confData.get(varname)));
    }

    public boolean getBoolean(String varname) {
        return (Boolean.parseBoolean(confData.get(varname)));
    }

    public void set(String varname, String value) {
        confData.put(varname, value);
    }

    public void set(String varname, int value) {
        confData.put(varname, Integer.toString(value));
    }

    public void set(String varname, long value) {
        confData.put(varname, Long.toString(value));
    }

    public void set(String varname, float value) {
        confData.put(varname, Float.toString(value));
    }

    public void set(String varname, boolean value) {
        confData.put(varname, Boolean.toString(value));
    }

}
