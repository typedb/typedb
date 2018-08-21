/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.GraknTx;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.reasoner.atom.binary.ResourceAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.util.Schema;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.neq;
import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.rolePlayer;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getValuePredicates;
import static ai.grakn.graql.internal.util.StringConverter.typeLabelToString;
import static java.util.stream.Collectors.joining;

/**
 * Represents the {@code has} property on an {@link Thing}.
 *
 * This property can be queried, inserted or deleted.
 *
 * The property is defined as a {@link Relationship} between an {@link Thing} and a {@link Attribute}, where the
 * {@link Attribute} is of a particular type.
 *
 * When matching, {@link  ai.grakn.util.Schema.EdgeLabel#ROLE_PLAYER} edges are used to speed up the traversal. The type of the {@link Relationship} does not
 * matter.
 *
 * When inserting, an implicit {@link Relationship} is created between the instance and the {@link Attribute}, using
 * type labels derived from the label of the {@link AttributeType}.
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class HasAttributeProperty extends AbstractVarProperty implements NamedProperty {

    public static final String NAME = "has";

    public static HasAttributeProperty of(Label attributeType, VarPatternAdmin attribute, VarPatternAdmin relationship) {
        attribute = attribute.isa(label(attributeType)).admin();
        return new AutoValue_HasAttributeProperty(attributeType, attribute, relationship);
    }

    public abstract Label type();
    public abstract VarPatternAdmin attribute();
    public abstract VarPatternAdmin relationship();

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        Stream.Builder<String> repr = Stream.builder();

        repr.add(typeLabelToString(type()));

        if (attribute().var().isUserDefinedName()) {
            repr.add(attribute().var().toString());
        } else {
            attribute().getProperties(ValueProperty.class).forEach(prop -> repr.add(prop.predicate().toString()));
        }

        if (hasReifiedRelationship()) {
            repr.add("via").add(relationship().getPrintableName());
        }

        return repr.build().collect(joining(" "));
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        Label type = type();
        Label has = Schema.ImplicitType.HAS.getLabel(type);
        Label key = Schema.ImplicitType.KEY.getLabel(type);

        Var edge1 = Graql.var();
        Var edge2 = Graql.var();

        return ImmutableSet.of(
                rolePlayer(this, relationship().var(), edge1, start, null, null, ImmutableSet.of(has, key)),
                rolePlayer(this, relationship().var(), edge2, attribute().var(), null, null, ImmutableSet.of(has, key)),
                neq(this, edge1, edge2)
        );
    }

    @Override
    public Stream<VarPatternAdmin> innerVarPatterns() {
        return Stream.of(attribute(), relationship());
    }

    @Override
    void checkValidProperty(GraknTx graph, VarPatternAdmin var) {
        SchemaConcept schemaConcept = graph.getSchemaConcept(type());
        if (schemaConcept == null) {
            throw GraqlQueryException.labelNotFound(type());
        }
        if(!schemaConcept.isAttributeType()) {
            throw GraqlQueryException.mustBeAttributeType(type());
        }
    }

    @Override
    public Collection<PropertyExecutor> insert(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Attribute attributeConcept = executor.get(attribute().var()).asAttribute();
            Thing thing = executor.get(var).asThing();
            ConceptId relationshipId = thing.relhas(attributeConcept).id();
            executor.builder(relationship().var()).id(relationshipId);
        };

        PropertyExecutor executor = PropertyExecutor.builder(method)
                .produces(relationship().var())
                .requires(var, attribute().var())
                .build();

        return ImmutableSet.of(executor);
    }

    @Override
    public Stream<VarPatternAdmin> getTypes() {
        return Stream.of(label(type()).admin());
    }

    private boolean hasReifiedRelationship() {
        return relationship().getProperties().findAny().isPresent() || relationship().var().isUserDefinedName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HasAttributeProperty that = (HasAttributeProperty) o;

        if (!type().equals(that.type())) return false;
        if (!attribute().equals(that.attribute())) return false;

        // TODO: Having to check this is pretty dodgy
        // This check is necessary for `equals` and `hashCode` because `VarPattern` equality is defined
        // s.t. `var() != var()`, but `var().label("movie") == var().label("movie")`
        // i.e., a `Var` is compared by name, but a `VarPattern` ignores the name if the var is not user-defined
        return !hasReifiedRelationship() || relationship().equals(that.relationship());
    }

    @Override
    public int hashCode() {
        int result = type().hashCode();
        result = 31 * result + attribute().hashCode();

        // TODO: Having to check this is pretty dodgy, explanation in #equals
        if (hasReifiedRelationship()) {
            result = 31 * result + relationship().hashCode();
        }

        return result;
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        //NB: HasAttributeProperty always has (type) label specified
        Var varName = var.var().asUserDefined();

        Var relationVariable = relationship().var();
        Var attributeVariable = attribute().var().asUserDefined();
        Set<ValuePredicate> predicates = getValuePredicates(attributeVariable, attribute(), vars, parent);

        IsaProperty isaProp = attribute().getProperties(IsaProperty.class).findFirst().orElse(null);
        VarPatternAdmin typeVar = isaProp != null? isaProp.type() : null;
        IdPredicate predicate = typeVar != null? getIdPredicate(attributeVariable, typeVar, vars, parent) : null;
        ConceptId predicateId = predicate != null? predicate.getPredicate() : null;

        //add resource atom
        VarPatternAdmin resVar = relationVariable.isUserDefinedName()?
                varName.has(type(), attributeVariable, relationVariable).admin() :
                varName.has(type(), attributeVariable).admin();
        return ResourceAtom.create(resVar, attributeVariable, relationVariable, predicateId, predicates, parent);
    }
}
