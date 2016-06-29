/**
 * Apr 22, 2013
 */
package com.hiido.hcat.common.err;

import java.io.IOException;

/**
 * @author lin
 * 
 */
public class RemoteException extends IOException {
    private static final long serialVersionUID = 1L;

    public RemoteException() {
        super();
    }

    public RemoteException(String message) {
        super(message);
    }

    public RemoteException(String message, Throwable cause) {
        super(message, cause);
    }

    public RemoteException(Throwable cause) {
        super(cause);
    }

}
