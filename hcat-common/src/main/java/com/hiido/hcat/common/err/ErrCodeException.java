/**
 * 2012-10-12
 */
package com.hiido.hcat.common.err;

public class ErrCodeException extends Exception {
    private static final long serialVersionUID = 1L;
    private final ErrCode code;

    public ErrCodeException(ErrCode code, String msg, Throwable ex) {
        super(msg, ex);
        this.code = code;
    }

    public ErrCodeException(ErrCode code, String msg) {
        super(msg, null);
        this.code = code;
    }

    public int intCode() {
        return code.code();
    }

    public ErrCode errCode() {
        return code;
    }

    public static ErrCodeException unknowErr(String msg, Throwable ex) {
        return new ErrCodeException(ErrCode.Unknow, msg, ex);
    }

    public static ErrCodeException IllegalAddr(String msg) {
        return new ErrCodeException(ErrCode.IllegalAddr, msg);
    }

    public static ErrCodeException ioErr(String msg) {
        return new ErrCodeException(ErrCode.IOErr, msg);
    }
    
    public static ErrCodeException ioErr(String msg, Throwable ex) {
        return new ErrCodeException(ErrCode.IOErr, msg, ex);
    }
    
    public static ErrCodeException conErr(String msg) {
        return new ErrCodeException(ErrCode.ConnectErr, msg);
    }
    
    public static ErrCodeException conErr(String msg, Throwable ex) {
        return new ErrCodeException(ErrCode.ConnectErr, msg, ex);
    }

    public static ErrCodeException authenticateErr(String msg) {
        return new ErrCodeException(ErrCode.AuthenticateErr, msg);
    }

    public static ErrCodeException authorizeErr(String msg, Throwable ex) {
        return new ErrCodeException(ErrCode.AuthorizeErr, msg, ex);
    }

    public static ErrCodeException authorizeErr(String msg) {
        return new ErrCodeException(ErrCode.AuthorizeErr, msg);
    }

    public static ErrCodeException verMismatchErr(String msg) {
        return new ErrCodeException(ErrCode.VerMismatchErr, msg);
    }

    public static ErrCodeException runErr(String msg, Throwable ex) {
        return new ErrCodeException(ErrCode.RunErr, msg, ex);
    }

    public static ErrCodeException runErr(Throwable ex) {
        return new ErrCodeException(ErrCode.RunErr, String.format("exception[%s]:%s", ex.getClass().getName(),
                ex.getMessage()), ex);
    }

    public static ErrCodeException runErr(String msg) {
        return new ErrCodeException(ErrCode.RunErr, msg);
    }

    public static ErrCodeException timeoutErr(String msg, Throwable ex) {
        return new ErrCodeException(ErrCode.TimeoutErr, msg, ex);
    }

    public static ErrCodeException remoteErr(String msg, Throwable ex) {
        return new ErrCodeException(ErrCode.RemoteErr, msg, ex);

    }

    public static ErrCodeException remoteErr(String msg) {
        return new ErrCodeException(ErrCode.RemoteErr, msg);
    }

    public static ErrCodeException unsupportedErr(String msg) {
        return new ErrCodeException(ErrCode.UnsupportedErr, msg);
    }

    public static ErrCodeException illegalStateErr(String msg) {
        return new ErrCodeException(ErrCode.IllegalStateErr, msg);
    }

    public static ErrCodeException sqlErr(String msg, Throwable ex) {
        return new ErrCodeException(ErrCode.SQLErr, msg, ex);
    }

    public static ErrCodeException argErr(String msg) {
        return new ErrCodeException(ErrCode.IllegalArg, msg);
    }

    public static ErrCodeException valueOf(ErrCode code, String msg, Throwable ex) {
        return new ErrCodeException(code, msg, ex);

    }

    public static ErrCodeException valueOf(ErrCode code, String msg) {
        return new ErrCodeException(code, msg);
    }
}
