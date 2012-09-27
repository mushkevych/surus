package com.reinvent.synergy.data.exception;

/**
 * @author Bohdan Mushkevych
 * Description: synergy exception meant to mark recoverable exceptions
 */
public class RecoverableException extends Exception {
    public RecoverableException(String s) {
        super(s);
    }

    public RecoverableException(Throwable throwable) {
        super(throwable);
    }
}
