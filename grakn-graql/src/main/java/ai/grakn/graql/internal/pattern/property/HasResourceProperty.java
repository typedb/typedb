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
import ai.grakn.concept.Label;
import ai.grakn.concept.Relation;
import ai.grakn.concept.Resource;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.binary.ResourceAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.util.Schema;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.neq;
import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.shortcut;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getValuePredicates;
import static ai.grakn.graql.internal.util.StringConverter.typeLabelToString;
import static java.util.stream.Collectors.joining;

/**
 * Represents the {@code has} property on an {@link Thing}.
 *
 * This property can be queried, inserted or deleted.
 *
 * The property is defined as a relationship between an {@link Thing} and a {@link Resource}, where the
 * {@link Resource} is of a particular type.
 *
 * When matching, shortcut edges are used to speed up the traversal. The type of the relationship does not matter.
 *
 * When inserting, an implicit relation is created between the instance and the resource, using type labels derived from
 * the label of the resource type.
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class HasResourceProperty extends AbstractVarProperty implements NamedProperty {

    public static HasResourceProperty of(Label resourceType, VarPatternAdmin resource) {
        resource = resource.isa(label(resourceType)).admin();
        return new AutoValue_HasResourceProperty(resourceType, resource);
    }

    public abstract Label type();

    public abstract VarPatternAdmin resource();

    @Override
    public String getName() {
        return "has";
    }

    @Override
    public String getProperty() {
        Stream.Builder<String> repr = Stream.builder();

        repr.add(typeLabelToString(type()));

        if (resource().getVarName().isUserDefinedName()) {
            repr.add(resource().getVarName().toString());
        } else {
            resource().getProperties(ValueProperty.class).forEach(prop -> repr.add(prop.predicate().toString()));
        }
        return repr.build().collect(joining(" "));
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        Var relation = Graql.var();
        Var edge1 = Graql.var();
        Var edge2 = Graql.var();

        return ImmutableSet.of(
                shortcut(this, relation, edge1, start, Optional.empty()),
                shortcut(this, relation, edge2, resource().getVarName(), Optional.empty()),
                neq(this, edge1, edge2)
        );
    }

    @Override
    public Stream<VarPatternAdmin> getInnerVars() {
        return Stream.of(resource());
    }

    @Override
    void checkValidProperty(GraknGraph graph, VarPatternAdmin var) {
        Type type = graph.getOntologyConcept(type());
        if(type == null || !type.isResourceType()) {
            throw GraqlQueryException.mustBeResourceType(type());
        }
    }

    @Override
    public void insert(Var var, InsertQueryExecutor executor) throws GraqlQueryException {
        Resource resourceConcept = executor.get(resource().getVarName()).asResource();
        Thing thing = executor.get(var).asThing();
        thing.resource(resourceConcept);
    }

    @Override
    public Set<Var> requiredVars(Var var) {
        return ImmutableSet.of(var, resource().getVarName());
    }

    @Override
    public void delete(GraknGraph graph, Concept concept) {
        Optional<ValuePredicateAdmin> predicate =
                resource().getProperties(ValueProperty.class).map(ValueProperty::predicate).findAny();

        Role owner = graph.getOntologyConcept(Schema.ImplicitType.HAS_OWNER.getLabel(type()));
        Role value = graph.getOntologyConcept(Schema.ImplicitType.HAS_VALUE.getLabel(type()));

        concept.asThing().relations(owner).stream()
                .filter(relation -> testPredicate(predicate, relation, value))
                .forEach(Concept::delete);
    }

    private boolean testPredicate(Optional<ValuePredicateAdmin> optPredicate, Relation relation, Role resourceRole) {
        Object value = relation.rolePlayers(resourceRole).iterator().next().asResource().getValue();

        return optPredicate
                .flatMap(ValuePredicateAdmin::getPredicate)
                .map(predicate -> predicate.test(value))
                .orElse(true);
    }

    @Override
    public Stream<VarPatternAdmin> getTypes() {
        return Stream.of(label(type()).admin());
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        Var varName = var.getVarName().asUserDefined();

        Label type = this.type();
        VarPatternAdmin resource = this.resource();
        Var resourceVariable = resource.getVarName().asUserDefined();
        Set<ValuePredicate> predicates = getValuePredicates(resourceVariable, resource, vars, parent);

        IsaProperty isaProp = resource.getProperties(IsaProperty.class).findFirst().orElse(null);
        VarPatternAdmin typeVar = isaProp != null? isaProp.type() : null;
        IdPredicate idPredicate = typeVar != null? getIdPredicate(resourceVariable, typeVar, vars, parent) : null;

        //add resource atom
        VarPatternAdmin resVar = varName.has(type, resourceVariable).admin();
        return new ResourceAtom(resVar, resourceVariable, idPredicate, predicates, parent);
    }
}
