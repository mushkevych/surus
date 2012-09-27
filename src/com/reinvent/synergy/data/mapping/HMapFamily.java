package com.reinvent.synergy.data.mapping;

import java.lang.annotation.*;

/**
 * @author Bohdan Mushkevych
 * Description: Annotation, presenting mapping between a Map and a <column family>
 * i.e. every entry in the Map will become sepapate <column> within the <column family>
 *
 * In case Value of the Map is Map itself - then additional HNestedMap annotation must be used to describe nested map
 * @see HNestedMap
 */

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface HMapFamily {
	String family();
	Class keyType();
    Class valueType();
}
