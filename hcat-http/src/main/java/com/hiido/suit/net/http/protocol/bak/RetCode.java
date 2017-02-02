/**
 * Oct 15, 2012
 */
package com.hiido.suit.net.http.protocol.bak;

import com.hiido.hcat.common.err.ErrCode;
import com.hiido.hcat.common.err.ErrCodeException;

public final class RetCode {
    public static final int CODE_OK = 0;
    private int code = CODE_OK;
    private String msg;

    public RetCode() {
    }

    public RetCode(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public static RetCode ok(String msg) {
        return new RetCode(CODE_OK, msg);
    }

    public static RetCode ok() {
        return new RetCode(CODE_OK, null);
    }

    public static RetCode err(int code, String msg) {
        return new RetCode(code, msg);
    }

    public boolean isOk() {
        return code == CODE_OK;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public void throwErrCodeException() throws ErrCodeException {
        if (!isOk()) {
            ErrCode c = ErrCode.valueOfInt(code);
            throw ErrCodeException.valueOf(c, msg);
        }
    }

}
