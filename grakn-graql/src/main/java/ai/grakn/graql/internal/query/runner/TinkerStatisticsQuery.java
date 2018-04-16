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

package ai.grakn.graql.internal.query.runner;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.GraknComputer;
import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.StatisticsQuery;
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

    private TinkerStatisticsQuery(EmbeddedGraknTx<?> tx, StatisticsQuery<?> query, GraknComputer computer) {
        super(tx, query, computer);
    }

    static TinkerStatisticsQuery create(EmbeddedGraknTx<?> tx, StatisticsQuery<?> query, GraknComputer computer) {
        return new TinkerStatisticsQuery(tx, query, computer);
    }

    final Set<Label> getCombinedSubTypes() {
        Set<Label> allSubTypes = getHasResourceRelationLabels(calcStatisticsResourceTypes());
        allSubTypes.addAll(subLabels());
        allSubTypes.addAll(query().attributeLabels());
        return allSubTypes;
    }

    final Set<Label> statisticsResourceLabels() {
        return calcStatisticsResourceTypes().stream()
                .map(SchemaConcept::getLabel)
                .collect(CommonUtil.toImmutableSet());
    }

    final boolean selectedResourceTypesHaveInstance() {
        for (Label resourceType : statisticsResourceLabels()) {
            for (Label type : subLabels()) {
                Boolean patternExist = tx().graql().infer(false).match(
                        Graql.var("x").has(resourceType, Graql.var()),
                        Graql.var("x").isa(Graql.label(type))
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

    final @Nullable AttributeType.DataType<?> getDataTypeOfSelectedResourceTypes() {
        AttributeType.DataType<?> dataType = null;
        for (Type type : calcStatisticsResourceTypes()) {
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
                    throw GraqlQueryException.resourcesWithDifferentDataTypes(query().attributeLabels());
                }
            }
        }
        return dataType;
    }

    private static Set<Label> getHasResourceRelationLabels(Set<Type> subTypes) {
        return subTypes.stream()
                .filter(Concept::isAttributeType)
                .map(resourceType -> Schema.ImplicitType.HAS.getLabel(resourceType.getLabel()))
                .collect(Collectors.toSet());
    }// If the sub graph contains attributes, we may need to add implicit relations to the paths

    private ImmutableSet<Type> calcStatisticsResourceTypes() {
        if (query().attributeLabels().isEmpty()) {
            throw GraqlQueryException.statisticsAttributeTypesNotSpecified();
        }

        return query().attributeLabels().stream()
                .map((label) -> {
                    Type type = tx().getSchemaConcept(label);
                    if (type == null) throw GraqlQueryException.labelNotFound(label);
                    if (!type.isAttributeType()) throw GraqlQueryException.mustBeAttributeType(type.getLabel());
                    return type;
                })
                .flatMap(Type::subs)
                .collect(CommonUtil.toImmutableSet());
    }
}
