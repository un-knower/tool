/**
 * Apr 22, 2013
 */
package com.hiido.hcat.common.err;

import java.io.IOException;

/**
 * @author lin
 * 
 */
public class ValidateException extends IOException {

    private static final long serialVersionUID = 1L;

    public ValidateException() {
        super();
    }

    public ValidateException(String message) {
        super(message);
    }

    public ValidateException(String message, Throwable cause) {
        super(message, cause);
    }

    public ValidateException(Throwable cause) {
        super(cause);
    }

}
