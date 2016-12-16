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

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Type;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;

import java.util.*;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.joining;

abstract class AbstractStatisticsQuery<T> extends AbstractComputeQuery<T> {

    Set<String> statisticsResourceTypeNames = new HashSet<>();
    Map<String, String> resourceTypesDataTypeMap = new HashMap<>();

    AbstractStatisticsQuery<T> setStatisticsResourceType(String... statisticsResourceTypeNames) {
        this.statisticsResourceTypeNames = Sets.newHashSet(statisticsResourceTypeNames);
        return this;
    }

    AbstractStatisticsQuery<T> setStatisticsResourceType(Collection<String> statisticsResourceTypeNames) {
        this.statisticsResourceTypeNames = Sets.newHashSet(statisticsResourceTypeNames);
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

    final String resourcesString() {
        return " of " + statisticsResourceTypeNames.stream().map(StringConverter::idToString).collect(joining(", "));
    }

    private void getResourceTypes(GraknGraph graph) {
        if (statisticsResourceTypeNames.isEmpty())
            throw new IllegalStateException(ErrorMessage.RESOURCE_TYPE_NOT_SPECIFIED.getMessage());

        Set<Type> statisticsResourceTypes = statisticsResourceTypeNames.stream().map((name) -> {
            Type type = graph.getType(name);
            if (type == null) throw new IllegalArgumentException(ErrorMessage.NAME_NOT_FOUND.getMessage(name));
            return type;
        }).collect(Collectors.toSet());
        for (Type type : statisticsResourceTypes) {
            type.subTypes().forEach(subtype -> this.statisticsResourceTypeNames.add(subtype.getName()));
        }

        ResourceType<?> metaResourceType = graph.admin().getMetaResourceType();
        metaResourceType.subTypes().stream()
                .filter(type -> !type.equals(metaResourceType))
                .forEach(type -> resourceTypesDataTypeMap.put(type.asType().getName(), type.asResourceType().getDataType().getName()));
    }

    String checkSelectedResourceTypesHaveCorrectDataType(Set<String> types) {
        if (types == null || types.isEmpty())
            throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                    .getMessage(this.getClass().toString()));

        String dataType = null;
        for (String type : types) {
            // check if the selected type is a resource-type
            if (!resourceTypesDataTypeMap.containsKey(type))
                throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                        .getMessage(this.getClass().toString()));

            if (dataType == null) {
                // check if the resource-type has data-type LONG or DOUBLE
                dataType = resourceTypesDataTypeMap.get(type);

                if (!dataType.equals(ResourceType.DataType.LONG.getName()) &&
                        !dataType.equals(ResourceType.DataType.DOUBLE.getName()))
                    throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                            .getMessage(this.getClass().toString()));

            } else {
                // check if all the resource-types have the same data-type
                if (!dataType.equals(resourceTypesDataTypeMap.get(type)))
                    throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                            .getMessage(this.getClass().toString()));
            }
        }
        return dataType;
    }

    boolean selectedResourceTypesHaveInstance(Set<String> statisticsResourceTypes) {

        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, this.keySpace).getGraph();

        List<Pattern> checkResourceTypes = statisticsResourceTypes.stream()
                .map(type -> var("x").has(type)).collect(Collectors.toList());
        List<Pattern> checkSubtypes = subTypeNames.stream()
                .map(type -> var("x").isa(type)).collect(Collectors.toList());

        return graph.graql().infer(false).match(or(checkResourceTypes), or(checkSubtypes)).ask().execute();
    }

    Set<String> getCombinedSubTypes() {
        Set<String> allSubTypes = statisticsResourceTypeNames.stream()
                .map(Schema.Resource.HAS_RESOURCE::getName).collect(Collectors.toSet());
        allSubTypes.addAll(subTypeNames);
        allSubTypes.addAll(statisticsResourceTypeNames);
        return allSubTypes;
    }
}
