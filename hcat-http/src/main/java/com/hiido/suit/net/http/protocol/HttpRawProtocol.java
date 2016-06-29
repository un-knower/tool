package com.hiido.suit.net.http.protocol;

import com.hiido.hcat.common.err.ErrCodeException;
import com.hiido.hcat.common.util.StringUtils;

public abstract class HttpRawProtocol implements HttpProtocol {
    private static final long serialVersionUID = 1L;

    public String toJsonString(){
        try {
            return StringUtils.obj2Json(this);
        } catch (Exception e) {
            throw new RuntimeException("obj to json failed:" + this.toString(), e);
        }
    }

    public byte[] toJsonBytes() throws ErrCodeException {
        try {
            return StringUtils.obj2JsonAsBytes(this);
        } catch (Exception e) {
            throw ErrCodeException.runErr("obj to json failed:" + this.toString(), e);
        }
    }
}
