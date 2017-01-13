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
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Printer;
import ai.grakn.graql.internal.analytics.CommonOLAP;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final int numberOfOntologyChecks = 10;

    static final Logger LOGGER = LoggerFactory.getLogger(ComputeQuery.class);

    Optional<GraknGraph> graph = Optional.empty();
    String keySpace;
    Set<TypeName> subTypeNames = new HashSet<>();

    @Override
    public ComputeQuery<T> withGraph(GraknGraph graph) {
        this.graph = Optional.of(graph);
        return this;
    }

    @Override
    public ComputeQuery<T> in(TypeName... subTypeNames) {
        this.subTypeNames = Sets.newHashSet(subTypeNames);
        return this;
    }

    @Override
    public ComputeQuery<T> in(Collection<TypeName> subTypeNames) {
        this.subTypeNames = Sets.newHashSet(subTypeNames);
        return this;
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

        // make sure we don't accidentally commit anything
        try {
            theGraph.rollback();
        } catch (UnsupportedOperationException ignored) {
            // TODO: Fix this properly. I.E. Don't run TinkerGraph Tests which hit this line.
        }
        getAllSubTypes(theGraph);
    }

    private void getAllSubTypes(GraknGraph graph) {
        // fetch all the types in the subGraph
        Set<Type> subGraph = subTypeNames.stream().map((name) -> {
            Type type = graph.getType(name);
            if (type == null) throw new IllegalArgumentException(ErrorMessage.NAME_NOT_FOUND.getMessage(name));
            return type;
        }).collect(Collectors.toSet());

        // get all types if subGraph is empty, else get all subTypes of each type in subGraph
        if (subGraph.isEmpty()) {
            EntityType metaEntityType = graph.admin().getMetaEntityType();
            metaEntityType.subTypes().forEach(type -> this.subTypeNames.add(type.asType().getName()));
            ResourceType<?> metaResourceType = graph.admin().getMetaResourceType(); //Yay for losing the type
            metaResourceType.subTypes().forEach(type -> this.subTypeNames.add(type.asType().getName()));
            RelationType metaRelationType = graph.admin().getMetaRelationType();
            metaRelationType.subTypes().forEach(type -> this.subTypeNames.add(type.asType().getName()));
            subTypeNames.remove(metaEntityType.getName());
            subTypeNames.remove(metaResourceType.getName());
            subTypeNames.remove(metaRelationType.getName());
            this.subTypeNames.removeAll(CommonOLAP.analyticsElements);
        } else {
            for (Type type : subGraph) {
                type.subTypes().forEach(subType -> this.subTypeNames.add(subType.getName()));
            }
        }
    }

    GraknComputer getGraphComputer() {
        return Grakn.factory(Grakn.DEFAULT_URI, keySpace).getGraphComputer();
    }

    boolean selectedTypesHaveInstance() {
        if (subTypeNames.isEmpty()) return false;

        List<Pattern> checkSubtypes = subTypeNames.stream()
                .map(type -> var("x").isa(type)).collect(Collectors.toList());
        return this.graph.get().graql().infer(false).match(or(checkSubtypes)).ask().execute();
    }

    boolean verticesExistInSubgraph(ConceptId... ids) {
        for (ConceptId id : ids) {
            Instance instance = this.graph.get().getConcept(id);
            if (instance == null || !subTypeNames.contains(instance.type().getName())) return false;
        }
        return true;
    }

    void mutateResourceOntology(TypeName resourceTypeName, ResourceType.DataType<?> resourceDataType) {
        GraknGraph theGraph = this.graph.get();

        ResourceType resource = theGraph.putResourceType(resourceTypeName, resourceDataType);
        for (TypeName type : subTypeNames) {
            theGraph.getType(type).hasResource(resource);
        }

        try {
            theGraph.commit();
        } catch (GraknValidationException e) {
            throw new RuntimeException(ErrorMessage.ONTOLOGY_MUTATION.getMessage(e.getMessage()), e);
        }
    }

    void waitOnMutateResourceOntology(TypeName resourceTypeName) {
        GraknGraph theGraph = this.graph.get();
        theGraph.showImplicitConcepts(true);

        for (int i = 0; i < numberOfOntologyChecks; i++) {
            boolean isOntologyComplete = true;
            try {
                theGraph.rollback();
            } catch (UnsupportedOperationException ignored) {
                // TODO: Fix this properly. I.E. Don't run TinkerGraph Tests which hit this line.
            }

            ResourceType resource = theGraph.getResourceType(resourceTypeName);
            if (resource == null) continue;
            RoleType degreeOwner = theGraph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceTypeName));
            if (degreeOwner == null) continue;
            RoleType degreeValue = theGraph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceTypeName));
            if (degreeValue == null) continue;
            RelationType relationType = theGraph.getRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceTypeName));
            if (relationType == null) continue;

            for (TypeName type : subTypeNames) {
                Collection<RoleType> roles = theGraph.getType(type).playsRoles();
                if (!roles.contains(degreeOwner)) {
                    isOntologyComplete = false;
                    break;
                }
            }

            if (isOntologyComplete) {
                theGraph.showImplicitConcepts(false);
                return;
            }
        }
        theGraph.showImplicitConcepts(false);
        throw new RuntimeException(
                ErrorMessage.ONTOLOGY_MUTATION
                        .getMessage("Failed to confirm ontology is present after mutation."));
    }

    abstract String graqlString();

    final String subtypeString() {
        if (subTypeNames.isEmpty()) {
            return ";";
        } else {
            return " in "
                    + subTypeNames.stream().map(type -> StringConverter.idToString(type.getValue())).collect(joining(", ")) + ";";
        }
    }

    @Override
    public String toString() {
        return "compute " + graqlString();
    }

    Set<TypeName> getHasResourceRelationTypes() {
        return subTypeNames.stream()
                .filter(type -> graph.get().getType(type).isResourceType())
                .map(Schema.Resource.HAS_RESOURCE::getName)
                .collect(Collectors.toSet());
    }
}
