package io.mindmaps.graql.internal;

/**
 * Some constant types that Graql needs to know about.
 * This is currently only the has-resource relationship, which is not automatically added to the ontology
 */
public enum GraqlType {

    /**
     * The id of the generic has-resource relationship, used for attaching resources to instances with the 'has' syntax
     */
    HAS_RESOURCE("has-%s"),

    /**
     * The id of a role in has-resource, played by the owner of the resource
     */
    HAS_RESOURCE_OWNER("has-%s-owner"),

    /**
     * The id of a role in has-resource, played by the resource
     */
    HAS_RESOURCE_VALUE("has-%s-value");

    private final String name;

    private GraqlType(String name) {
        this.name = name;
    }

    public String getId(String resourceTypeId) {
        return String.format(name, resourceTypeId);
    }
}
