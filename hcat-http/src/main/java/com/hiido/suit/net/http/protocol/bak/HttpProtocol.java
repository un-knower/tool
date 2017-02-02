/**
 * Mar 5, 2013
 */
package com.hiido.suit.net.http.protocol.bak;

import java.io.Serializable;

import com.hiido.hcat.common.err.ErrCodeException;

public interface HttpProtocol extends Serializable {
    String name();

    String toJsonString() throws ErrCodeException;

    byte[] toJsonBytes() throws ErrCodeException;
}
