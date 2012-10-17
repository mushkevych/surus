package com.reinvent.surus.mapping;

import java.lang.annotation.*;

/**
 * @author Bohdan Mushkevych
 * Description: Annotation describes simple property that will be stored as a <column> in <column family>
 *
 * HProperty must NOT be used in conjunction with other annotations
 */

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface HProperty {
    /**
     * @return String name of the Column Family
     */
    String family();

    /**
     * @return String name of the Column (qualifier)
     */
	String identifier();
}
