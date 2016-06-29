/**
 * Sep 18, 2012
 */
package com.hiido.hcat.common;

/**
 * @author lin
 * 
 */
public class Pair<F, S> {
    private F first;
    private S second;

    public Pair(F f, S s) {
        this.first = f;
        this.second = s;
    }

    public Pair() {
    }

    public F getFirst() {
        return first;
    }

    public S getSecond() {
        return second;
    }

    public void setFirst(F f) {
        this.first = f;
    }

    public void setSecond(S s) {
        this.second = s;
    }

    public static <F,S>Pair<F,S> of(F f,S s){
        return new Pair<F, S>(f, s);
    }
    @Override
    public String toString() {
        return String.format("f=%s,s=%s", first, second);
    }
}
