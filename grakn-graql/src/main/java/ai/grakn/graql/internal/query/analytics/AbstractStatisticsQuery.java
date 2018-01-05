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

import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Label;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.joining;

abstract class AbstractStatisticsQuery<T> extends AbstractComputeQuery<T> {

    Set<Label> statisticsResourceLabels = new HashSet<>();
    Set<Type> statisticsResourceTypes = new HashSet<>();

    AbstractStatisticsQuery<T> setStatisticsResourceType(String... statisticsResourceTypeLabels) {
        this.statisticsResourceLabels =
                Arrays.stream(statisticsResourceTypeLabels).map(Label::of).collect(Collectors.toSet());
        return this;
    }

    AbstractStatisticsQuery<T> setStatisticsResourceType(Collection<Label> statisticsResourceLabels) {
        this.statisticsResourceLabels = Sets.newHashSet(statisticsResourceLabels);
        return this;
    }

    @Override
    public boolean isStatisticsQuery() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    void getAllSubTypes() {
        super.getAllSubTypes();
        getResourceTypes(tx.get());
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

    private void getResourceTypes(GraknTx graph) {
        if (statisticsResourceLabels.isEmpty()) {
            throw GraqlQueryException.statisticsAttributeTypesNotSpecified();
        }

        statisticsResourceTypes = statisticsResourceLabels.stream()
                .map((label) -> {
                    Type type = graph.getSchemaConcept(label);
                    if (type == null) throw GraqlQueryException.labelNotFound(label);
                    if (!type.isAttributeType()) throw GraqlQueryException.mustBeAttributeType(type.getLabel());
                    return type;
                })
                .flatMap(Type::subs)
                .collect(Collectors.toSet());
        statisticsResourceLabels = statisticsResourceTypes.stream()
                .map(SchemaConcept::getLabel)
                .collect(Collectors.toSet());
    }

    @Nullable
    AttributeType.DataType getDataTypeOfSelectedResourceTypes() {
        AttributeType.DataType dataType = null;
        for (Type type : statisticsResourceTypes) {
            // check if the selected type is a resource-type
            if (!type.isAttributeType()) throw GraqlQueryException.mustBeAttributeType(type.getLabel());
            AttributeType resourceType = (AttributeType) type;
            if (dataType == null) {
                // check if the resource-type has data-type LONG or DOUBLE
                dataType = resourceType.getDataType();
                if (!dataType.equals(AttributeType.DataType.LONG) &&
                        !dataType.equals(AttributeType.DataType.DOUBLE)) {
                    throw GraqlQueryException.resourceMustBeANumber(dataType, resourceType.getLabel());
                }

            } else {
                // check if all the resource-types have the same data-type
                if (!dataType.equals(resourceType.getDataType())) {
                    throw GraqlQueryException.resourcesWithDifferentDataTypes(statisticsResourceLabels);
                }
            }
        }
        return dataType;
    }

    boolean selectedResourceTypesHaveInstance(Set<Label> statisticsResourceTypes) {
        for (Label resourceType : statisticsResourceTypes) {
            for (Label type : subLabels) {
                Boolean patternExist = tx.get().graql().infer(false).match(
                        var("x").has(resourceType, var()),
                        var("x").isa(Graql.label(type))
                ).iterator().hasNext();
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
//        return tx.get().graql().infer(false)
//                .match(or(checkResourceTypes), or(checkSubtypes)).aggregate(ask()).execute();
    }

    Set<Label> getCombinedSubTypes() {
        Set<Label> allSubTypes = getHasResourceRelationLabels(statisticsResourceTypes);
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

        return statisticsResourceLabels.equals(that.statisticsResourceLabels);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + statisticsResourceLabels.hashCode();
        return result;
    }
}
