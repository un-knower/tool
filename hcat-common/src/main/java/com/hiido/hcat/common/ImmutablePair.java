/**
 * Mar 14, 2013
 */
package com.hiido.hcat.common;

/**
 * @author lin
 * 
 */
public final class ImmutablePair<F, S> extends Pair<F, S> {
    public static final ImmutablePair<Integer, String> zero = new ImmutablePair<Integer, String>(0, null);

    public ImmutablePair(F f, S s) {
        super(f, s);
    }

    @Override
    public void setFirst(F f) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSecond(S s) {
        throw new UnsupportedOperationException();
    }
}
