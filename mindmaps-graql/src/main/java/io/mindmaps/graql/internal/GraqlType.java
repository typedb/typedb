package io.mindmaps.graql.internal;

/**
 * Some constant types that Graql needs to know about.
 * This is currently only the has-resource relationship, which is not automatically added to the ontology
 */
public class GraqlType {

    private GraqlType() {

    }

    /**
     * The id of the generic has-resource relationship, used for attaching resources to instances with the 'has' syntax
     */
    public static final String HAS_RESOURCE = "has-resource";

    /**
     * The id of a role in has-resource, played by the owner of the resource
     */
    public static final String HAS_RESOURCE_TARGET = "has-resource-target";

    /**
     * The id of a role in has-resource, played by the resource
     */
    public static final String HAS_RESOURCE_VALUE = "has-resource-value";
}
