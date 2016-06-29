package com.hiido.hcat.service;

import javax.servlet.http.HttpServletRequest;

import com.hiido.hcat.common.err.ErrCode;
import com.hiido.hcat.common.err.ErrCodeException;
import com.hiido.suit.Business.BusType;
import com.hiido.suit.CipherUser;
import com.hiido.suit.SuitUser;
import com.hiido.suit.common.util.ServletUtil;
import com.hiido.suit.security.CipherKeyVerifier;
import com.hiido.suit.security.SecurityCenter;


public abstract class CoreServer {
	
	protected  int port;
    protected  int safePort;

	protected void verifyRemoteAddr(SecurityCenter scenter, SuitUser user, BusType busType, HttpServletRequest request, boolean verifyLocalIp)
            throws ErrCodeException {
        String raddr = "unknow";
        String laddr = "unknow";
        int localPort = request.getLocalPort();
        String msg = null;
        boolean safe = (localPort == safePort);
        try {
            raddr = ServletUtil.getDirectClientAddress(request);
            boolean legal = scenter.legalAddr(true, user, busType, raddr, false);
            if (!legal) {
                msg = String.format("illegal,remote addr[%s],user[%s]，safe[%s] ", raddr, user, safe);
                throw ErrCodeException.IllegalAddr(msg);
            }
            if (verifyLocalIp) {
                laddr = user.getLocalIP();
                legal = scenter.legalAddr(true, user, busType, laddr, true);
                if (!legal) {
                    msg = String.format("illegal,local addr[%s],user[%s]，safe[%s] ", laddr, user, safe);
                    throw ErrCodeException.IllegalAddr(msg);
                }
            }
        } catch (ErrCodeException e) {
            throw e;
        } catch (Exception e) {
            msg = String.format("failed to verify remoteAddr[%s],localAddr[%s],user[%s],safe[%s]:%s", raddr, laddr,
                    user, safe, e.getMessage());
            throw ErrCodeException.runErr(msg, e);
        }
    }
	
    protected void verifyUser(SecurityCenter scenter, CipherKeyVerifier cuseVerifier, CipherUser cuser,
            BusType busType, String skey, HttpServletRequest request, boolean veriferCipher, boolean veriferAddr, boolean verifyLocalIp)
            throws ErrCodeException {
        SuitUser suser = cuser.getSuitUser();
        if (veriferAddr) {
            verifyRemoteAddr(scenter, suser, busType, request, verifyLocalIp);
        }
        if (veriferCipher) {
            try {
				cuseVerifier.verifer(cuser, skey, suser.getBusUser());
			} catch (com.hiido.suit.err.ErrCodeException e) {
				throw new ErrCodeException(ErrCode.AuthenticateErr, "failed to verifer.", e);
			}
        }
    }
}
