package com.reinvent.synergy.data.exception;

/**
 * @author Bohdan Mushkevych
 * date 28/09/11
 * Description: synergy exception meant to mark unrecoverable exceptions that stops processing
 */
public class UnrecoverableException extends Exception {
    public UnrecoverableException(String s) {
        super(s);
    }

    public UnrecoverableException(Throwable throwable) {
        super(throwable);
    }
}
