/**
 * Aug 10, 2012
 */
package com.hiido.suit.net.http.protocol.bak;

import com.hiido.hcat.common.err.ErrCode;
import com.hiido.hcat.common.err.ErrCodeException;

public class RetProtocolReply extends RawProtocolReply {
    private static final long serialVersionUID = 1L;

    protected int retCode = RetCode.CODE_OK;
    protected String retMessage;

    public boolean sucess() {
        return retCode == RetCode.CODE_OK;
    }

    public void setRet(int code, String msg) {
        this.retCode = code;
        this.retMessage = msg;
    }

    public int getRetCode() {
        return retCode;
    }

    public void setRetCode(int rtcode) {
        this.retCode = rtcode;
    }

    public String getRetMessage() {
        return retMessage;
    }

    public void setRetMessage(String rtMessage) {
        this.retMessage = rtMessage;
    }

    @Override
    public void checkSucess() throws ErrCodeException {
        if (!sucess()) {
            ErrCode err = ErrCode.valueOfInt(retCode);
            throw ErrCodeException.valueOf(err, retMessage);
        }
    }

    public void unknow(String msg) {
        this.retCode = ErrCode.Unknow.code();
        this.retMessage = msg;
    }

    public void mismatchVersion(String msg) {
        this.retCode = ErrCode.VerMismatchErr.code();
        this.retMessage = msg;
    }

    public void err(String msg) {
        retCode = ErrCode.RunErr.code();
        this.retMessage = msg;
    }

}
