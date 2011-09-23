package com.reinvent.synergy.data.mapping;

import java.lang.annotation.*;

/**
 * @author Bohdan Mushkevych
 * date: 02/09/11
 * Description:
 */

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface HMapProperty {
	String family();
	Class keyType();
    Class valueType();
}
