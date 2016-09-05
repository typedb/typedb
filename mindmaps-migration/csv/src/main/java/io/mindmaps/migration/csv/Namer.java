package io.mindmaps.migration.csv;

/**
 * A Namer provides functionality for mapping from CSV table and column names to the corresponding elements in a
 * Mindmaps ontology. This interface provides default implementations.
 */
public interface Namer {

    static final String RESOURCE_NAME = "%s-resource";

    /**
     * Generate the name of the resource. Appending "resource" avoids conflicts with entity names.
     */
    default String resourceName(String type) {
        return String.format(RESOURCE_NAME, type);
    }
}
