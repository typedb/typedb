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
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Printer;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.joining;

abstract class AbstractComputeQuery<T> implements ComputeQuery<T> {

    static final Logger LOGGER = LoggerFactory.getLogger(ComputeQuery.class);

    Optional<GraknTx> tx = Optional.empty();
    private GraknComputer graknComputer = null;
    boolean includeAttribute = false;

    Set<Label> subLabels = new HashSet<>();
    Set<Type> subTypes = new HashSet<>();

    @Override
    public ComputeQuery<T> withTx(GraknTx tx) {
        this.tx = Optional.of(tx);
        return this;
    }

    @Override
    public ComputeQuery<T> in(String... subTypeLabels) {
        this.subLabels = Arrays.stream(subTypeLabels).map(Label::of).collect(Collectors.toSet());
        return this;
    }

    @Override
    public ComputeQuery<T> in(Collection<Label> subLabels) {
        this.subLabels = Sets.newHashSet(subLabels);
        return this;
    }

    @Override
    public ComputeQuery<T> includeAttribute() {
        this.includeAttribute = true;
        return this;
    }

    @Override
    public void kill() {
        if (graknComputer != null) {
            graknComputer.killJobs();
        }
    }

    @Override
    public Stream<String> resultsString(Printer printer) {
        Object computeResult = execute();
        if (computeResult instanceof Map) {
            if (((Map) computeResult).isEmpty()) {
                return Stream.of("There are no instances of the selected type(s).");
            }
            if (((Map) computeResult).values().iterator().next() instanceof Set) {
                Map<?, ?> map = (Map) computeResult;
                return map.entrySet().stream().map(entry -> {
                    StringBuilder stringBuilder = new StringBuilder();
                    for (Object s : (Iterable) entry.getValue()) {
                        stringBuilder.append(entry.getKey()).append("\t").append(s).append("\n");
                    }
                    return stringBuilder.toString();
                });
            }
        }
        return Stream.of(printer.graqlString(computeResult));
    }

    void initSubGraph() {
        if (this.isStatisticsQuery()) {
            includeAttribute = true;
        }
    }

    void getAllSubTypes() {
        // get all types if subGraph is empty, else get all subTypes of each type in subGraph
        // only include attributes and implicit "has-xxx" relationships when user specifically asked for them
        GraknTx graknTx = tx.get();
        if (subLabels.isEmpty()) {
            if (includeAttribute) {
                graknTx.admin().getMetaConcept().subs().forEach(subTypes::add);
            } else {
                graknTx.admin().getMetaEntityType().subs().forEach(subTypes::add);
                graknTx.admin().getMetaRelationType().subs()
                        .filter(relationshipType -> !relationshipType.isImplicit()).forEach(subTypes::add);
            }
        } else {
            subTypes = subLabels.stream().map(label -> {
                SchemaConcept type = graknTx.getSchemaConcept(label);
                if (type == null) throw GraqlQueryException.labelNotFound(label);
                if (!type.isType()) {
                    throw GraqlQueryException.cannotGetInstancesOfNonType(type.getLabel());
                }
                if (!includeAttribute && (type.isAttributeType() || type.isImplicit())) {
                    includeAttribute = true;
                }
                return type.asType();
            }).collect(Collectors.toSet());

            if (includeAttribute) {
                subTypes = subTypes.stream().flatMap(Type::subs).collect(Collectors.toSet());
            } else {
                subTypes = subTypes.stream().flatMap(Type::subs)
                        .filter(relationshipType -> !relationshipType.isImplicit()).collect(Collectors.toSet());
            }
        }
        subLabels = subTypes.stream().map(SchemaConcept::getLabel).collect(Collectors.toSet());
    }

    GraknComputer getGraphComputer() {
        if (graknComputer == null) {
            if(tx.isPresent()){
                graknComputer = tx.get().session().getGraphComputer();
            } else {
                throw new IllegalStateException("Transaction has not been provided. Cannot initialise graph computer");
            }
        }
        return graknComputer;
    }

    boolean selectedTypesHaveInstance() {
        if (subLabels.isEmpty()) {
            LOGGER.info("No types found while looking for instances");
            return false;
        }

        List<Pattern> checkSubtypes = subLabels.stream()
                .map(type -> var("x").isa(Graql.label(type))).collect(Collectors.toList());
        return this.tx.get().graql().infer(false).match(or(checkSubtypes)).iterator().hasNext();
    }

    boolean verticesExistInSubgraph(ConceptId... ids) {
        for (ConceptId id : ids) {
            Thing thing = this.tx.get().getConcept(id);
            if (thing == null || !subLabels.contains(thing.type().getLabel())) return false;
        }
        return true;
    }

    abstract String graqlString();

    final String subtypeString() {
        return subLabels.isEmpty() ? ";" : " in "
                + subLabels.stream().map(StringConverter::typeLabelToString).collect(joining(", ")) + ";";
    }

    @Override
    public String toString() {
        return "compute " + graqlString();
    }

    Set<LabelId> getRolePlayerLabelIds() {
        return subTypes.stream()
                .filter(Concept::isRelationshipType)
                .map(Concept::asRelationshipType)
                .filter(RelationshipType::isImplicit)
                .flatMap(RelationshipType::relates)
                .flatMap(Role::playedByTypes)
                .map(type -> tx.get().admin().convertToId(type.getLabel()))
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractComputeQuery<?> that = (AbstractComputeQuery<?>) o;

        return tx.equals(that.tx) && includeAttribute == that.includeAttribute && subLabels.equals(that.subLabels);
    }

    @Override
    public int hashCode() {
        int result = tx.hashCode();
        result = 31 * result + Boolean.hashCode(includeAttribute);
        result = 31 * result + subLabels.hashCode();
        return result;
    }

    Set<LabelId> convertLabelsToIds(Set<Label> labelSet) {
        return labelSet.stream()
                .map(tx.get().admin()::convertToId)
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

    static Set<Label> getHasResourceRelationLabels(Set<Type> subTypes) {
        return subTypes.stream()
                .filter(Concept::isAttributeType)
                .map(resourceType -> Schema.ImplicitType.HAS.getLabel(resourceType.getLabel()))
                .collect(Collectors.toSet());
    }

    static String getRandomJobId() {
        return Integer.toString(ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    }
}
