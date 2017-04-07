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
import ai.grakn.GraknComputer;
import ai.grakn.GraknGraph;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Printer;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.ErrorMessage;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.joining;

abstract class AbstractComputeQuery<T> implements ComputeQuery<T> {

    static final Logger LOGGER = LoggerFactory.getLogger(ComputeQuery.class);

    Optional<GraknGraph> graph = Optional.empty();
    GraknComputer graknComputer = null;
    String keySpace;
    Set<TypeLabel> subTypeLabels = new HashSet<>();

    @Override
    public ComputeQuery<T> withGraph(GraknGraph graph) {
        this.graph = Optional.of(graph);
        return this;
    }

    @Override
    public ComputeQuery<T> in(String... subTypeLabels) {
        this.subTypeLabels = Arrays.stream(subTypeLabels).map(TypeLabel::of).collect(Collectors.toSet());
        return this;
    }

    @Override
    public ComputeQuery<T> in(Collection<TypeLabel> subTypeLabels) {
        this.subTypeLabels = Sets.newHashSet(subTypeLabels);
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
        GraknGraph theGraph = graph.orElseThrow(() -> new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage()));
        keySpace = theGraph.getKeyspace();

        getAllSubTypes(theGraph);
    }

    private void getAllSubTypes(GraknGraph graph) {
        // fetch all the types in the subGraph
        Set<Type> subGraph = subTypeLabels.stream().map((label) -> {
            Type type = graph.getType(label);
            if (type == null) throw new IllegalArgumentException(ErrorMessage.LABEL_NOT_FOUND.getMessage(label));
            return type;
        }).collect(Collectors.toSet());

        // get all types if subGraph is empty, else get all subTypes of each type in subGraph
        if (subGraph.isEmpty()) {
            EntityType metaEntityType = graph.admin().getMetaEntityType();
            metaEntityType.subTypes().forEach(type -> this.subTypeLabels.add(type.asType().getLabel()));
            ResourceType<?> metaResourceType = graph.admin().getMetaResourceType(); //Yay for losing the type
            metaResourceType.subTypes().forEach(type -> this.subTypeLabels.add(type.asType().getLabel()));
            RelationType metaRelationType = graph.admin().getMetaRelationType();
            metaRelationType.subTypes().forEach(type -> this.subTypeLabels.add(type.asType().getLabel()));
            subTypeLabels.remove(metaEntityType.getLabel());
            subTypeLabels.remove(metaResourceType.getLabel());
            subTypeLabels.remove(metaRelationType.getLabel());
        } else {
            for (Type type : subGraph) {
                type.subTypes().forEach(subType -> this.subTypeLabels.add(subType.getLabel()));
            }
        }
    }

    GraknComputer getGraphComputer() {
        if (graknComputer == null) {
            graknComputer = Grakn.session(Grakn.DEFAULT_URI, keySpace).getGraphComputer();
        }
        return graknComputer;
    }

    boolean selectedTypesHaveInstance() {
        if (subTypeLabels.isEmpty()) return false;

        List<Pattern> checkSubtypes = subTypeLabels.stream()
                .map(type -> var("x").isa(Graql.label(type))).collect(Collectors.toList());
        return this.graph.get().graql().infer(false).match(or(checkSubtypes)).ask().execute();
    }

    boolean verticesExistInSubgraph(ConceptId... ids) {
        for (ConceptId id : ids) {
            Instance instance = this.graph.get().getConcept(id);
            if (instance == null || !subTypeLabels.contains(instance.type().getLabel())) return false;
        }
        return true;
    }

    abstract String graqlString();

    final String subtypeString() {
        if (subTypeLabels.isEmpty()) {
            return ";";
        } else {
            return " in "
                    + subTypeLabels.stream().map(StringConverter::typeLabelToString).collect(joining(", ")) + ";";
        }
    }

    @Override
    public String toString() {
        return "compute " + graqlString();
    }

    Set<TypeLabel> getHasResourceRelationTypes() {
        return subTypeLabels.stream()
                .filter(type -> graph.get().getType(type).isResourceType())
                .map(Schema.ImplicitType.HAS::getLabel)
                .collect(Collectors.toSet());
    }
}
