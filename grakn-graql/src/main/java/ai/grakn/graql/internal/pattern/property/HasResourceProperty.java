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
import ai.grakn.concept.TypeName;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.name;
import static ai.grakn.graql.internal.reasoner.Utility.getValuePredicates;
import static java.util.stream.Collectors.joining;

public class HasResourceProperty extends AbstractVarProperty implements NamedProperty {

    private final Optional<TypeName> resourceType;
    private final VarAdmin resource;

    public HasResourceProperty(VarAdmin resource) {
        this.resourceType = Optional.empty();
        this.resource = resource.isa(Schema.MetaSchema.RESOURCE.getName().getValue()).admin();
    }

    public HasResourceProperty(TypeName resourceType, VarAdmin resource) {
        this.resourceType = Optional.of(resourceType);
        this.resource = resource.isa(resourceType.getValue()).admin();
    }

    public Optional<TypeName> getType() {
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

        resourceType.ifPresent(type -> repr.add(type.getValue()));

        if (resource.isUserDefinedName()) {
            repr.add(resource.getPrintableName());
        } else {
            resource.getProperties(ValueProperty.class).forEach(prop -> repr.add(prop.getPredicate().toString()));
        }
        return repr.build().collect(joining(" "));
    }

    @Override
    public Collection<EquivalentFragmentSet> match(VarName start) {
        return Sets.newHashSet(EquivalentFragmentSet.create(
                Fragments.shortcut(Optional.empty(), Optional.empty(), Optional.empty(), start, resource.getVarName()),
                Fragments.shortcut(Optional.empty(), Optional.empty(), Optional.empty(), resource.getVarName(), start)
        ));
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(resource);
    }

    @Override
    void checkValidProperty(GraknGraph graph, VarAdmin var) {
        if (resourceType.isPresent() && graph.getType(resourceType.get()) == null) {
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

        TypeName type = resourceType.orElseThrow(() -> failDelete(this));

        RoleType owner = graph.getType(Schema.Resource.HAS_RESOURCE_OWNER.getName(type));
        RoleType value = graph.getType(Schema.Resource.HAS_RESOURCE_VALUE.getName(type));

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

        return resourceType.equals(that.resourceType) && resource.equals(that.resource);

    }

    @Override
    public int hashCode() {
        int result = resourceType.hashCode();
        result = 31 * result + resource.hashCode();
        return result;
    }

    @Override
    public Atomic mapToAtom(VarAdmin var, Set<VarAdmin> vars, ReasonerQuery parent) {
        VarName varName = var.getVarName();
        Optional<TypeName> type = this.getType();
        VarAdmin valueVar = this.getResource();
        VarName valueVariable = valueVar.isUserDefinedName() ?
                valueVar.getVarName() : varName.map(name -> name + "-" + type.orElse(null) + "-" + UUID.randomUUID().toString());
        Set<Predicate> predicates = getValuePredicates(valueVariable, valueVar, vars, parent);

        //add resource atom
        Var resource = Graql.var(valueVariable);
        VarAdmin resVar = type
                .map(t ->Graql.var(varName).has(t.getValue(), resource))
                .orElseGet(() -> Graql.var(varName).has(resource)).admin();
        return new ai.grakn.graql.internal.reasoner.atom.binary.Resource(resVar, predicates, parent);
    }
}
