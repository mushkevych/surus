package com.reinvent.synergy.data.rest;

/**
 * @author Bohdan Mushkevych
 * date 27/01/12
 * Description: holds static fields, commonly used in request handlers
 */
public interface RequestHandler extends Runnable {
    public static final String PARAMETER_DOMAIN_ID = "domain_name";
    public static final String PARAMETER_KEYWORD_ID = "keyword";
    public static final String PARAMETER_KEYWORD_VALUE = "keyword_value";
}
