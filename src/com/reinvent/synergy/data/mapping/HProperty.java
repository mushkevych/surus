package com.reinvent.synergy.data.mapping;

import java.lang.annotation.*;

/**
 * @author Bohdan Mushkevych
 * date: 02/09/11
 * Description: Annotation describes simple property that will be stored as a <column> in <column family>
 *
 * HProperty must NOT be used in conjunction with other annotations
 */

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface HProperty {
	String family();
	String identifier();
}
