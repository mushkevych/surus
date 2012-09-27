package com.reinvent.synergy.data.mapping;

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
    String family();
    String identifier();
	Class keyType();
    Class valueType();
}
