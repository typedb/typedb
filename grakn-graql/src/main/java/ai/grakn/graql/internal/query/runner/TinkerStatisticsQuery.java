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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query.runner;

import ai.grakn.GraknComputer;
import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.analytics.StatisticsQuery;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Execution context for a {@link GraknTx}, {@link GraknComputer} and a {@link StatisticsQuery}.
 *
 * @author Felix Chapman
 */
class TinkerStatisticsQuery extends TinkerComputeQuery<StatisticsQuery<?>> {

    TinkerStatisticsQuery(EmbeddedGraknTx<?> tx, StatisticsQuery<?> query) {
        super(tx, query);
    }

    final Set<Label> fullScopeTypeLabels() {
        Set<Label> allTypeLabels = getHasAttributeRelationLabels(ofTypes());
        allTypeLabels.addAll(inTypeLabels());
        allTypeLabels.addAll(query().ofTypes());
        return allTypeLabels;
    }

    final ImmutableSet<Type> ofTypes() {
        if (query().ofTypes().isEmpty()) {
            throw GraqlQueryException.statisticsAttributeTypesNotSpecified();
        }

        return query().ofTypes().stream()
                .map((label) -> {
                    Type type = tx().getSchemaConcept(label);
                    if (type == null) throw GraqlQueryException.labelNotFound(label);
                    if (!type.isAttributeType()) throw GraqlQueryException.mustBeAttributeType(type.getLabel());
                    return type;
                })
                .flatMap(Type::subs)
                .collect(CommonUtil.toImmutableSet());
    }

    final Set<Label> ofTypeLabels() {
        return ofTypes().stream()
                .map(SchemaConcept::getLabel)
                .collect(CommonUtil.toImmutableSet());
    }

    final boolean ofTypesHaveInstances() {
        for (Label attributeType : ofTypeLabels()) {
            for (Label type : inTypeLabels()) {
                Boolean patternExist = tx().graql().infer(false).match(
                        Graql.var("x").has(attributeType, Graql.var()),
                        Graql.var("x").isa(Graql.label(type))
                ).iterator().hasNext();
                if (patternExist) return true;
            }
        }
        return false;
        //TODO: should use the following ask query when ask query is even lazier
//        List<Pattern> checkResourceTypes = statisticsResourceTypes.stream()
//                .map(type -> var("x").has(type, var())).collect(Collectors.toList());
//        List<Pattern> checkSubtypes = inTypes.stream()
//                .map(type -> var("x").isa(Graql.label(type))).collect(Collectors.toList());
//
//        return tx.get().graql().infer(false)
//                .match(or(checkResourceTypes), or(checkSubtypes)).aggregate(ask()).execute();
    }

    @Nullable
    final AttributeType.DataType<?> validateAndGetDataTypes() {
        AttributeType.DataType<?> dataType = null;
        for (Type type : ofTypes()) {
            // check if the selected type is a attribute type
            if (!type.isAttributeType()) throw GraqlQueryException.mustBeAttributeType(type.getLabel());
            AttributeType<?> attributeType = type.asAttributeType();
            if (dataType == null) {
                // check if the attribute type has data-type LONG or DOUBLE
                dataType = attributeType.getDataType();
                if (!dataType.equals(AttributeType.DataType.LONG) &&
                        !dataType.equals(AttributeType.DataType.DOUBLE)) {
                    throw GraqlQueryException.attributeMustBeANumber(dataType, attributeType.getLabel());
                }

            } else {
                // check if all the attribute types have the same data-type
                if (!dataType.equals(attributeType.getDataType())) {
                    throw GraqlQueryException.attributesWithDifferentDataTypes(query().ofTypes());
                }
            }
        }
        return dataType;
    }

    private static Set<Label> getHasAttributeRelationLabels(Set<Type> types) {
        // If the sub graph contains attributes, we may need to add implicit relations to the path
        return types.stream()
                .filter(Concept::isAttributeType)
                .map(attributeType -> Schema.ImplicitType.HAS.getLabel(attributeType.getLabel()))
                .collect(Collectors.toSet());
    }
}
