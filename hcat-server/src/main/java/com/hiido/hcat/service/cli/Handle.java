package com.hiido.hcat.service.cli;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.hiido.suit.CipherUser;
import com.hiido.suit.err.ErrCodeException;

/**
 * for client
 * @author zrc
 *
 */
public abstract class Handle {
	protected Map<String, String> cipher = new HashMap<String, String>();
	
	public Map<String, String> getCipher() {
		return cipher;
	}
	
	public void setCipher(Map<String, String> cipher) {
		this.cipher = cipher;
	}
	
	public void fromCipher(CipherUser cuser) throws ErrCodeException {
		Map<String, String> map = new HashMap<String, String>();
        try {
            cuser.put2Map(map, true);
        } catch (IOException e) {
            throw ErrCodeException.ioErr(e.getMessage(), e);
        }
        setCipher(map);
	}
	
}
