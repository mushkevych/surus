package com.reinvent.synergy.data.mapping;

import java.lang.annotation.*;

/**
 * @author Bohdan Mushkevych
 * date: 21/09/11
 * Description: Annotation to be used for complex structures like:
 * public Map<String, Map<String, Integer>> keyword;
 *
 * In this case nested Map<String, Integer> will be stored in HBase as Json document.
 * Such document needs to be later serialized/deserialized with notion of the key/value types
 *
 * HNestedMap must be used in conjunction with HMapProperty annotation
 */

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface HNestedMap {
	Class keyType();
    Class valueType();
}
