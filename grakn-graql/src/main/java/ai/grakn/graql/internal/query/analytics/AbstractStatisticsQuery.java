/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.joining;

abstract class AbstractStatisticsQuery<T, V extends ComputeQuery<T>>
        extends AbstractComputeQuery<T, V> {

    private ImmutableSet<Label> statisticsResourceLabels = ImmutableSet.of();

    AbstractStatisticsQuery(Optional<GraknTx> tx) {
        super(tx);
    }

    public V of(String... statisticsResourceTypeLabels) {
        return of(Arrays.stream(statisticsResourceTypeLabels).map(Label::of).collect(toImmutableSet()));
    }

    public V of(Collection<Label> statisticsResourceLabels) {
        this.statisticsResourceLabels = ImmutableSet.copyOf(statisticsResourceLabels);
        return (V) this;
    }

    @Override
    public boolean isStatisticsQuery() {
        return true;
    }

    @Override
    void getAllSubTypes(GraknTx tx) {
        super.getAllSubTypes(tx);
        getResourceTypes(tx);
    }

    final Set<Label> statisticsResourceLabels() {
        return statisticsResourceLabels;
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

    private void getResourceTypes(GraknTx tx) {
        statisticsResourceLabels = calcStatisticsResourceTypes(tx).stream()
                .map(SchemaConcept::getLabel)
                .collect(toImmutableSet());
    }

    @Nullable
    AttributeType.DataType<?> getDataTypeOfSelectedResourceTypes(GraknTx tx) {
        AttributeType.DataType<?> dataType = null;
        for (Type type : calcStatisticsResourceTypes(tx)) {
            // check if the selected type is a resource-type
            if (!type.isAttributeType()) throw GraqlQueryException.mustBeAttributeType(type.getLabel());
            AttributeType<?> resourceType = type.asAttributeType();
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

    boolean selectedResourceTypesHaveInstance(GraknTx tx, Set<Label> statisticsResourceTypes) {
        for (Label resourceType : statisticsResourceTypes) {
            for (Label type : subLabels()) {
                Boolean patternExist = tx.graql().infer(false).match(
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

    Set<Label> getCombinedSubTypes(GraknTx tx) {
        Set<Label> allSubTypes = getHasResourceRelationLabels(calcStatisticsResourceTypes(tx));
        allSubTypes.addAll(subLabels());
        allSubTypes.addAll(statisticsResourceLabels);
        return allSubTypes;
    }

    private ImmutableSet<Type> calcStatisticsResourceTypes(GraknTx tx) {
        if (statisticsResourceLabels.isEmpty()) {
            throw GraqlQueryException.statisticsAttributeTypesNotSpecified();
        }

        return statisticsResourceLabels.stream()
                .map((label) -> {
                    Type type = tx.getSchemaConcept(label);
                    if (type == null) throw GraqlQueryException.labelNotFound(label);
                    if (!type.isAttributeType()) throw GraqlQueryException.mustBeAttributeType(type.getLabel());
                    return type;
                })
                .flatMap(Type::subs)
                .collect(toImmutableSet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        AbstractStatisticsQuery<?, ?> that = (AbstractStatisticsQuery<?, ?>) o;

        return statisticsResourceLabels.equals(that.statisticsResourceLabels);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + statisticsResourceLabels.hashCode();
        return result;
    }
}
