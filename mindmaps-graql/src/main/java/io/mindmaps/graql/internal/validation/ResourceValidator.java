package io.mindmaps.graql.internal.validation;

import io.mindmaps.core.dao.MindmapsTransaction;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * A validator for validating resource types
 */
class ResourceValidator implements Validator {

    private final Collection<String> resourceTypes;

    /**
     * @param resourceTypes a list of resource type IDs to validate
     */
    public ResourceValidator(Collection<String> resourceTypes) {
        this.resourceTypes = resourceTypes;
    }

    @Override
    public Stream<String> getErrors(MindmapsTransaction transaction) {
        return resourceTypes.stream().flatMap(r -> validateResource(transaction, r));
    }

    /**
     * @param transaction the transaction to look up the resource type in
     * @param resourceType the resource type to validate
     * @return a stream of errors regarding this resource type
     */
    private Stream<String> validateResource(MindmapsTransaction transaction, String resourceType) {
        if (transaction.getResourceType(resourceType) == null) {
            return Stream.of(ErrorMessage.MUST_BE_RESOURCE_TYPE.getMessage(resourceType));
        } else {
            return Stream.empty();
        }
    }
}
