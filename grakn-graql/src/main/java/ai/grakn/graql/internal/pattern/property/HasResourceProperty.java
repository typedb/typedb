/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.util.GraqlType;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.util.GraqlType;
import ai.grakn.util.ErrorMessage;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.id;
import static ai.grakn.graql.internal.util.CommonUtil.tryAny;

public class HasResourceProperty extends AbstractVarProperty implements NamedProperty {

    private final String resourceType;
    private final VarAdmin resource;

    public HasResourceProperty(String resourceType, VarAdmin resource) {
        this.resourceType = resourceType;
        this.resource = resource.isa(resourceType).admin();
    }

    public String getType() {
        return resourceType;
    }

    public VarAdmin getResource() {
        return resource;
    }

    @Override
    public String getName() {
        return "has";
    }

    @Override
    public String getProperty() {
        String resourceRepr;
        if (resource.isUserDefinedName()) {
            resourceRepr = " " + resource.getPrintableName();
        } else {
            resourceRepr = tryAny(resource.getValuePredicates()).map(predicate -> " " + predicate).orElse("");
        }
        return resourceType + resourceRepr;
    }

    @Override
    public Collection<EquivalentFragmentSet> match(String start) {
        Optional<String> hasResource = Optional.of(GraqlType.HAS_RESOURCE.getId(resourceType));
        Optional<String> hasResourceOwner = Optional.of(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType));
        Optional<String> hasResourceValue = Optional.of(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType));

        return Sets.newHashSet(EquivalentFragmentSet.create(
                Fragments.shortcut(hasResource, hasResourceOwner, hasResourceValue, start, resource.getName()),
                Fragments.shortcut(hasResource, hasResourceValue, hasResourceOwner, resource.getName(), start)
        ));
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(resource);
    }

    @Override
    void checkValidProperty(GraknGraph graph, VarAdmin var) {
        if (graph.getResourceType(resourceType) == null) {
            throw new IllegalStateException(ErrorMessage.MUST_BE_RESOURCE_TYPE.getMessage(resourceType));
        }
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
        Resource resourceConcept = insertQueryExecutor.getConcept(resource).asResource();
        Instance instance = concept.asInstance();

        ResourceType type = resourceConcept.type();

        GraknGraph graph = insertQueryExecutor.getGraph();

        RelationType hasResource = graph.putRelationType(GraqlType.HAS_RESOURCE.getId(type.getId()));
        RoleType hasResourceTarget = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(type.getId()));
        RoleType hasResourceValue = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(type.getId()));

        Relation relation = graph.addRelation(hasResource);
        relation.putRolePlayer(hasResourceTarget, instance);
        relation.putRolePlayer(hasResourceValue, resourceConcept);
    }

    @Override
    public void delete(GraknGraph graph, Concept concept) {
        Optional<ValuePredicateAdmin> predicate = resource.getValuePredicates().stream().findAny();

        resources(concept).stream()
                .filter(r -> r.type().getId().equals(resourceType))
                .filter(r -> predicate.map(p -> p.getPredicate().test(r.getValue())).orElse(true))
                .forEach(Concept::delete);
    }

    @Override
    public Stream<VarAdmin> getTypes() {
        return Stream.of(id(resourceType).admin());
    }

    /**
     * @return all resources on the given concept
     */
    private Collection<Resource<?>> resources(Concept concept) {
        // Get resources attached to a concept
        // This method is necessary because the 'resource' method appears in 3 separate interfaces
        if (concept.isEntity()) {
            return concept.asEntity().resources();
        } else if (concept.isRelation()) {
            return concept.asRelation().resources();
        } else if (concept.isRule()) {
            return concept.asRule().resources();
        } else {
            return new HashSet<>();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HasResourceProperty that = (HasResourceProperty) o;

        if (!resourceType.equals(that.resourceType)) return false;
        return resource.equals(that.resource);

    }

    @Override
    public int hashCode() {
        int result = resourceType.hashCode();
        result = 31 * result + resource.hashCode();
        return result;
    }
}
