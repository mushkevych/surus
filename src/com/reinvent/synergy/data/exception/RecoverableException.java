package com.reinvent.synergy.data.exception;

/**
 * @author Bohdan Mushkevych
 * date 28/09/11
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
