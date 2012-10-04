package com.reinvent.surus.mapping;

import java.lang.annotation.*;

/**
 * @author Bohdan Mushkevych
 * Description: Annotation describes simple list that will be stored as a <column> in <column family>
 *
 * HProperty must NOT be used in conjunction with other annotations
 */

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface HListProperty {
	String family();
	String identifier();
    Class elementType();
}
