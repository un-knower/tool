package com.hiido.suit.net.http.protocol;

import java.util.Map;

public class TNRule extends HttpRawProtocol{
    
    private static final long serialVersionUID = 2638947543577383196L;
    public static final String P_NAME = "tnrule";
    public static final String PATH = "/" + P_NAME;
    
    @Override
    public String name() {
        return P_NAME;
    }
    
    public static final class Reply extends RawProtocolReply {
        private Map<String,Map<String, String>> mapping ;
        private boolean success;
        
        public Reply(){}
        
        public Map<String, Map<String, String>> getMapping() {
            return mapping;
        }

        public void setMapping(Map<String, Map<String, String>> mapping) {
            this.mapping = mapping;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }
    }
}
