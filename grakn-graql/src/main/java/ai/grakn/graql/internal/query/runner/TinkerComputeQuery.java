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
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.analytics.ComputeQuery;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Execution context for a {@link GraknTx}, {@link GraknComputer} and a {@link ComputeQuery}.
 *
 * @author Felix Chapman
 */
class TinkerComputeQuery<Q extends ComputeQuery<?>> {

    private final EmbeddedGraknTx<?> tx;
    private final Q query;
    private final GraknComputer computer;

    TinkerComputeQuery(EmbeddedGraknTx<?> tx, Q query) {
        this.tx = tx;
        this.query = query;
        this.computer = tx.session().getGraphComputer();
    }

    protected final GraknTx tx() {
        return tx;
    }

    protected final Q query() {
        return query;
    }

    public final ComputerResult compute(
            @Nullable VertexProgram<?> program, @Nullable MapReduce<?, ?, ?, ?, ?> mapReduce,
            @Nullable Set<LabelId> types, Boolean includesRolePlayerEdges
    ) {
        return computer.compute(program, mapReduce, types, includesRolePlayerEdges);
    }

    public final ComputerResult compute(@Nullable VertexProgram<?> program,
                                        @Nullable MapReduce<?, ?, ?, ?, ?> mapReduce,
                                        @Nullable Set<LabelId> types) {

        return computer.compute(program, mapReduce, types);
    }

    @Nullable
    private Thing getConcept(String conceptId) {
        return tx.getConcept(ConceptId.of(conceptId));
    }

    final Set<LabelId> getRolePlayerLabelIds() {
        return inTypes()
                .filter(Concept::isRelationshipType)
                .map(Concept::asRelationshipType)
                .filter(RelationshipType::isImplicit)
                .flatMap(RelationshipType::relates)
                .flatMap(Role::playedByTypes)
                .map(type -> tx.convertToId(type.getLabel()))
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

    final Stream<Type> inTypes() {
        // get all types if subGraph is empty, else get all inTypes of each type in subGraph
        // only include attributes and implicit "has-xxx" relationships when user specifically asked for them
        if (query.inTypes().isEmpty()) {
            ImmutableSet.Builder<Type> subTypesBuilder = ImmutableSet.builder();

            if (isAttributeIncluded()) {
                tx.admin().getMetaConcept().subs().forEach(subTypesBuilder::add);
            } else {
                tx.admin().getMetaEntityType().subs().forEach(subTypesBuilder::add);
                tx.admin().getMetaRelationType().subs()
                        .filter(relationshipType -> !relationshipType.isImplicit()).forEach(subTypesBuilder::add);
            }

            return subTypesBuilder.build().stream();
        } else {
            Stream<Type> subTypes = query.inTypes().stream().map(label -> {
                Type type = tx.getType(label);
                if (type == null) throw GraqlQueryException.labelNotFound(label);
                return type;
            }).flatMap(Type::subs);

            if (!isAttributeIncluded()) {
                subTypes = subTypes.filter(relationshipType -> !relationshipType.isImplicit());
            }

            return subTypes;
        }
    }

    final ImmutableSet<Label> inTypeLabels() {
        return inTypes().map(SchemaConcept::getLabel).collect(CommonUtil.toImmutableSet());
    }

    final boolean inTypesHaveInstances() {
        if (inTypeLabels().isEmpty()) return false;
        List<Pattern> checkSubtypes = inTypeLabels().stream()
                .map(type -> Graql.var("x").isa(Graql.label(type))).collect(Collectors.toList());

        return tx.graql().infer(false).match(Graql.or(checkSubtypes)).iterator().hasNext();
    }

    final boolean inTypesContainConcepts(ConceptId... ids) {
        for (ConceptId id : ids) {
            Thing thing = tx.getConcept(id);
            if (thing == null || !inTypeLabels().contains(thing.type().getLabel())) return false;
        }
        return true;
    }

    final boolean inTypesContainImplicitOrAttributeTypes() {
        return query.inTypes().stream().anyMatch(label -> {
            SchemaConcept type = tx.getSchemaConcept(label);
            return (type != null && (type.isAttributeType() || type.isImplicit()));
        });
    }

    final boolean isAttributeIncluded() {
        return query.isAttributeIncluded() || inTypesContainImplicitOrAttributeTypes();
    }
}
