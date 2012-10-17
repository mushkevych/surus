package com.reinvent.surus.mapping;

import java.lang.annotation.*;

/**
 * @author Bohdan Mushkevych
 * Description: Annotation to be used for data model Map properties, that occupy single <column> within a <column family>
 * object will be stored as JSON String in a separate <column> of a <column family>:
 * public Map<String, Integer> adIndex;
 *
 * HMapProperty must NOT be used in conjunction with other annotations
 */

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface HMapProperty {
    /**
     * @return String name of the Column Family
     */
    String family();

    /**
     * @return String name of the Column (qualifier)
     */
	String identifier();

    /**
     * @return For given {@code Map<K, V>} method returns {@code K.class}
     */
	Class keyType();

    /**
     * @return For given {@code Map<K, V>} method returns {@code V.class}
     */
    Class valueType();
}
