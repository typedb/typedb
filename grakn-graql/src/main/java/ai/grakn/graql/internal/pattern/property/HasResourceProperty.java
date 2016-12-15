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

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.Resource;
import ai.grakn.concept.RoleType;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.name;
import static java.util.stream.Collectors.joining;

public class HasResourceProperty extends AbstractVarProperty implements NamedProperty {

    private final Optional<String> resourceType;
    private final VarAdmin resource;

    public HasResourceProperty(VarAdmin resource) {
        this.resourceType = Optional.empty();
        this.resource = resource.isa(Schema.MetaSchema.RESOURCE.getName()).admin();
    }

    public HasResourceProperty(String resourceType, VarAdmin resource) {
        this.resourceType = Optional.of(resourceType);
        this.resource = resource.isa(resourceType).admin();
    }

    public Optional<String> getType() {
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
        Stream.Builder<String> repr = Stream.builder();

        resourceType.ifPresent(repr);

        if (resource.isUserDefinedName()) {
            repr.add(resource.getPrintableName());
        } else {
            resource.getProperties(ValueProperty.class).forEach(prop -> repr.add(prop.getPredicate().toString()));
        }
        return repr.build().collect(joining(" "));
    }

    @Override
    public Collection<EquivalentFragmentSet> match(String start) {
        Optional<String> hasResource = resourceType.map(Schema.Resource.HAS_RESOURCE::getName);
        Optional<String> hasResourceOwner = resourceType.map(Schema.Resource.HAS_RESOURCE_OWNER::getName);
        Optional<String> hasResourceValue = resourceType.map(Schema.Resource.HAS_RESOURCE_VALUE::getName);

        return Sets.newHashSet(EquivalentFragmentSet.create(
                Fragments.shortcut(hasResource, hasResourceOwner, hasResourceValue, start, resource.getVarName()),
                Fragments.shortcut(hasResource, hasResourceValue, hasResourceOwner, resource.getVarName(), start)
        ));
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(resource);
    }

    @Override
    void checkValidProperty(GraknGraph graph, VarAdmin var) {
        if (resourceType.isPresent() && graph.getResourceType(resourceType.get()) == null) {
            throw new IllegalStateException(ErrorMessage.MUST_BE_RESOURCE_TYPE.getMessage(resourceType));
        }
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
        Resource resourceConcept = insertQueryExecutor.getConcept(resource).asResource();
        Instance instance = concept.asInstance();
        instance.hasResource(resourceConcept);
    }

    @Override
    public void delete(GraknGraph graph, Concept concept) {
        Optional<ValuePredicateAdmin> predicate =
                resource.getProperties(ValueProperty.class).map(ValueProperty::getPredicate).findAny();

        String type = resourceType.orElseThrow(() -> failDelete(this));

        RoleType owner = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(type));
        RoleType value = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(type));

        concept.asInstance().relations(owner).stream()
                .filter(relation -> testPredicate(predicate, relation, value))
                .forEach(Concept::delete);
    }

    private boolean testPredicate(Optional<ValuePredicateAdmin> optPredicate, Relation relation, RoleType resourceRole) {
        Object value = relation.rolePlayers().get(resourceRole).asResource().getValue();

        return optPredicate
                .flatMap(ValuePredicateAdmin::getPredicate)
                .map(predicate -> predicate.test(value))
                .orElse(true);
    }

    @Override
    public Stream<VarAdmin> getTypes() {
        if (resourceType.isPresent()) {
            return Stream.of(name(resourceType.get()).admin());
        } else {
            return Stream.empty();
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
