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
    /**
     * @return String name of the Column Family
     */
    String family();

    /**
     * @return String name of the Column (qualifier)
     */
	String identifier();

    /**
     * @return type of the List elements. In other words, for given {@code List<T>} method returns {@code T.class}
     */
    Class elementType();
}
