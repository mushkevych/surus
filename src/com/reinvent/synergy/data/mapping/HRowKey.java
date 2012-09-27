package com.reinvent.synergy.data.mapping;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Bohdan Mushkevych
 * Description: Annotation marking byte[] primary key
 *
 * HRowKey must NOT be used in conjunction with other annotations
 * and there may be only 1 occurance per data model
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface HRowKey {
}