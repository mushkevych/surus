package com.reinvent.surus.mapping;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Bohdan Mushkevych
 * Description: Annotation marking byte[] primary key
 *
 * Constraints:
 * - There MUST be 1 and only 1 occurance per data model
 * - HRowKey MUST contain array of HFieldComponent to describe complex Row Keys
 *
 * In case your RowKey should be parsed by third-party component, use following declaration systax
 * {@code
 * .    @HRowKey(components = {
 * .        @HFieldComponent(name = Constants.KEY, length = HFieldComponent.LENGTH_VARIABLE, type = byte[].class)
 * .    })
 * .    byte[] key;
 * }
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface HRowKey {
    HFieldComponent[] components();
}