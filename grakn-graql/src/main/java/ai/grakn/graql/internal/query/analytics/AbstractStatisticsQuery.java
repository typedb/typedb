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

import ai.grakn.API;
import ai.grakn.GraknComputer;
import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.StatisticsQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.joining;

abstract class AbstractStatisticsQuery<T, V extends StatisticsQuery<T>>
        extends AbstractComputeQuery<T, V> implements StatisticsQuery<T> {

    private ImmutableSet<Label> statisticsResourceLabels = ImmutableSet.of();

    AbstractStatisticsQuery(Optional<GraknTx> tx) {
        super(tx);
    }

    @API
    public final V of(String... statisticsResourceTypeLabels) {
        return of(Arrays.stream(statisticsResourceTypeLabels).map(Label::of).collect(toImmutableSet()));
    }

    @API
    public final V of(Collection<Label> statisticsResourceLabels) {
        this.statisticsResourceLabels = ImmutableSet.copyOf(statisticsResourceLabels);
        return (V) this;
    }

    public final Collection<? extends Label> attributeLabels() {
        return statisticsResourceLabels;
    }

    @Override
    public boolean isStatisticsQuery() {
        return true;
    }

    final Set<Label> statisticsResourceLabels(GraknTx tx) {
        return calcStatisticsResourceTypes(tx).stream()
                .map(SchemaConcept::getLabel)
                .collect(toImmutableSet());
    }

    @Override
    final String graqlString() {
        return getName() + resourcesString() + subtypeString();
    }

    final <S> Optional<S> execWithMapReduce(GraknTx tx, GraknComputer computer, MapReduceFactory<S> mapReduceFactory) {
        AttributeType.DataType<?> dataType = getDataTypeOfSelectedResourceTypes(tx);
        if (!selectedResourceTypesHaveInstance(tx, statisticsResourceLabels(tx))) return Optional.empty();
        Set<LabelId> allSubLabelIds = convertLabelsToIds(tx, getCombinedSubTypes(tx));
        Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tx, statisticsResourceLabels(tx));

        GraknMapReduce<S> mapReduce =
                mapReduceFactory.get(statisticsResourceLabelIds, dataType, DegreeVertexProgram.DEGREE);

        ComputerResult result = computer.compute(
                new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                mapReduce,
                allSubLabelIds);
        Map<Serializable, S> map = result.memory().get(mapReduce.getClass().getName());

        LOGGER.debug("Result = " + map.get(MapReduce.NullObject.instance()));
        return Optional.of(map.get(MapReduce.NullObject.instance()));
    }

    interface MapReduceFactory<S> {
        GraknMapReduce<S> get(
                Set<LabelId> statisticsResourceLabelIds, AttributeType.DataType<?> dataType, String degreePropertyKey);
    }

    abstract String getName();

    private String resourcesString() {
        return " of " + statisticsResourceLabels.stream()
                .map(StringConverter::typeLabelToString).collect(joining(", "));
    }

    private static Set<Label> getHasResourceRelationLabels(Set<Type> subTypes) {
        return subTypes.stream()
                .filter(Concept::isAttributeType)
                .map(resourceType -> Schema.ImplicitType.HAS.getLabel(resourceType.getLabel()))
                .collect(Collectors.toSet());
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

    final boolean selectedResourceTypesHaveInstance(GraknTx tx, Set<Label> statisticsResourceTypes) {
        for (Label resourceType : statisticsResourceTypes) {
            for (Label type : subLabels(tx)) {
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

    final Set<Label> getCombinedSubTypes(GraknTx tx) {
        Set<Label> allSubTypes = getHasResourceRelationLabels(calcStatisticsResourceTypes(tx));
        allSubTypes.addAll(subLabels(tx));
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
