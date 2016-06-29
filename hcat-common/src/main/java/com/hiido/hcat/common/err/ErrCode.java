/**
 * 2012-10-13
 */
package com.hiido.hcat.common.err;

public enum ErrCode {
    Unknow(1), IOErr(2), AuthenticateErr(3), AuthorizeErr(4), VerMismatchErr(5), ProcotolErr(6), RunErr(7), RunTimeErr(
            8), IllegalAddr(9), TimeoutErr(10), FileNotFount(11), RemoteErr(12), UnsupportedErr(13), IllegalStateErr(14), SQLErr(
            15), IllegalArg(16), RedirectErr(17), ConnectErr(18);
    private final int code;

    private ErrCode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static ErrCode valueOfInt(int code) {
        switch (code) {
        case 1:
            return Unknow;
        case 2:
            return IOErr;
        case 3:
            return AuthenticateErr;
        case 4:
            return AuthorizeErr;
        case 5:
            return VerMismatchErr;
        case 6:
            return ProcotolErr;
        case 7:
            return RunErr;
        case 8:
            return RunTimeErr;
        case 9:
            return IllegalAddr;
        case 10:
            return TimeoutErr;
        case 11:
            return FileNotFount;
        case 12:
            return RemoteErr;
        case 13:
            return UnsupportedErr;
        case 14:
            return IllegalStateErr;
        case 15:
            return SQLErr;
        case 16:
            return IllegalArg;
        case 17:
            return RedirectErr;
        case 18:
        	return ConnectErr;
        default:
            throw new IllegalArgumentException("unsupported code:" + code);
        }
    }
}