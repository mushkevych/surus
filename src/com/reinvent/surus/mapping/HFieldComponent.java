package com.reinvent.surus.mapping;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Bohdan Mushkevych
 * Data structure describing Complex Field components
 * For instance:
 * - row key may consist of two components: timeperiod [int] + user_name [String]
 * - regular column value may consist of two integers: student_id [int] + year_of_birth [int]
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface HFieldComponent {
    public static final int LENGTH_VARIABLE = -1;

    /**
     * @return String-name of the component
     */
    String name();

    /**
     * @return length in bytes of the component
     * for primitive types, see org.apache.hadoop.hbase.util.Bytes.SIZEOF_*
     *
     * in case field contains only one component of varying length,
     * then its length should be set to LENGTH_VARIABLE (-1)
     * Note - in case more than 1 component is set to LENGTH_VARIABLE
     * an IllegalArgumentException will be thrown during key parsing
     * @see #LENGTH_VARIABLE
     */
    int length();

    /**
     * @return type of the component
     */
    Class type();
}
