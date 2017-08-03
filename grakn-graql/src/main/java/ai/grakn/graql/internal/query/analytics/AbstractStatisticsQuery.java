/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Label;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.joining;

abstract class AbstractStatisticsQuery<T> extends AbstractComputeQuery<T> {

    Set<Label> statisticsResourceLabels = new HashSet<>();
    private final Map<Label, ResourceType.DataType> resourceTypesDataTypeMap = new HashMap<>();

    AbstractStatisticsQuery<T> setStatisticsResourceType(String... statisticsResourceTypeLabels) {
        this.statisticsResourceLabels = Arrays.stream(statisticsResourceTypeLabels).map(Label::of).collect(Collectors.toSet());
        return this;
    }

    AbstractStatisticsQuery<T> setStatisticsResourceType(Collection<Label> statisticsResourceLabels) {
        this.statisticsResourceLabels = Sets.newHashSet(statisticsResourceLabels);
        return this;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    void initSubGraph() {
        super.initSubGraph();
        getResourceTypes(graph.get());
    }

    @Override
    final String graqlString() {
        return getName() + resourcesString() + subtypeString();
    }

    abstract String getName();

    private String resourcesString() {
        return " of " + statisticsResourceLabels.stream()
                .map(StringConverter::typeLabelToString).collect(joining(", "));
    }

    private void getResourceTypes(GraknGraph graph) {
        if (statisticsResourceLabels.isEmpty()) {
            throw GraqlQueryException.statisticsResourceTypesNotSpecified();
        }

        Set<Type> statisticsResourceTypes = statisticsResourceLabels.stream().map((label) -> {
            Type type = graph.getOntologyConcept(label);
            if (type == null) throw GraqlQueryException.labelNotFound(label);
            return type;
        }).collect(Collectors.toSet());
        for (Type type : statisticsResourceTypes) {
            type.subs().forEach(subtype -> this.statisticsResourceLabels.add(subtype.getLabel()));
        }

        ResourceType<?> metaResourceType = graph.admin().getMetaResourceType();
        metaResourceType.subs().stream()
                .filter(type -> !type.equals(metaResourceType))
                .forEach(type -> resourceTypesDataTypeMap.put(type.getLabel(), type.getDataType()));
    }

    @Nullable
    ResourceType.DataType getDataTypeOfSelectedResourceTypes(Set<Label> resourceTypes) {
        assert resourceTypes != null && !resourceTypes.isEmpty();

        ResourceType.DataType dataType = null;
        for (Label resourceType : resourceTypes) {
            // check if the selected type is a resource-type
            if (!resourceTypesDataTypeMap.containsKey(resourceType)) {
                throw GraqlQueryException.mustBeResourceType(resourceType);
            }

            if (dataType == null) {
                // check if the resource-type has data-type LONG or DOUBLE
                dataType = resourceTypesDataTypeMap.get(resourceType);

                if (!dataType.equals(ResourceType.DataType.LONG) &&
                        !dataType.equals(ResourceType.DataType.DOUBLE)) {
                    throw GraqlQueryException.resourceMustBeANumber(dataType, resourceType);
                }

            } else {
                // check if all the resource-types have the same data-type
                if (!dataType.equals(resourceTypesDataTypeMap.get(resourceType))) {
                    throw GraqlQueryException.resourcesWithDifferentDataTypes(resourceTypes);
                }
            }
        }
        return dataType;
    }

    boolean selectedResourceTypesHaveInstance(Set<Label> statisticsResourceTypes) {
        for (Label resourceType:statisticsResourceTypes){
            for (Label type:subLabels) {
                Boolean patternExist = graph.get().graql().infer(false).match(
                        var("x").has(resourceType, var()),
                        var("x").isa(Graql.label(type))
                ).ask().execute();
                if (patternExist) return true;
            }
        }
        return false;
        //TODO: should use the following ask query when ask query is even lazier
//        List<Pattern> checkResourceTypes = statisticsResourceTypes.stream()
//                .map(type -> var("x").has(type, var())).collect(Collectors.toList());
//        List<Pattern> checkSubtypes = subLabels.stream()
//                .map(type -> var("x").isa(Graql.label(type))).collect(Collectors.toList());
//
//        return graph.get().graql().infer(false)
//                .match(or(checkResourceTypes), or(checkSubtypes)).ask().execute();
    }

    Set<Label> getCombinedSubTypes() {
        Set<Label> allSubTypes = statisticsResourceLabels.stream()
                .map(Schema.ImplicitType.HAS::getLabel).collect(Collectors.toSet());
        allSubTypes.addAll(subLabels);
        allSubTypes.addAll(statisticsResourceLabels);
        return allSubTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        AbstractStatisticsQuery<?> that = (AbstractStatisticsQuery<?>) o;

        return statisticsResourceLabels.equals(that.statisticsResourceLabels) &&
                resourceTypesDataTypeMap.equals(that.resourceTypesDataTypeMap);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + statisticsResourceLabels.hashCode();
        result = 31 * result + resourceTypesDataTypeMap.hashCode();
        return result;
    }
}
