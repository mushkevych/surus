package com.reinvent.surus.mapping;

import java.lang.annotation.*;

/**
 * @author Bohdan Mushkevych
 * Description: Annotation to be used for complex structures like:
 * public Map<String, Map<String, Integer>> keyword;
 *
 * In this case nested Map<String, Integer> will be stored in HBase as Json document.
 * Such document needs to be later serialized/deserialized with notion of the key/value types
 *
 * HNestedMap must be used in conjunction with HMapFamily annotation
 * @see HMapFamily
 */

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface HNestedMap {
	Class keyType();
    Class valueType();
}
