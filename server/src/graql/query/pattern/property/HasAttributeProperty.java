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

package grakn.core.graql.query.pattern.property;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;

import java.util.Collection;
import java.util.stream.Stream;

import static grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets.neq;
import static grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets.rolePlayer;
import static grakn.core.graql.query.pattern.Pattern.label;
import static grakn.core.graql.util.StringUtil.typeLabelToString;
import static java.util.stream.Collectors.joining;

/**
 * Represents the {@code has} property on an Thing. This property can be queried, inserted or deleted.
 * The property is defined as a Relationship between an Thing and a Attribute,
 * where theAttribute is of a particular type. When matching,  Schema.EdgeLabel#ROLE_PLAYER
 * edges are used to speed up the traversal. The type of the Relationship does notmatter.
 * When inserting, an implicit Relationship is created between the instance and the Attribute,
 * using type labels derived from the label of the AttributeType.
 */
public class HasAttributeProperty extends VarProperty {

    public static final String NAME = "has";

    private final Label type;
    private final Statement attribute;
    private final Statement relationship;

    public HasAttributeProperty(Label type, Statement attribute, Statement relationship) {
        attribute = attribute.isa(label(type));
        if (type == null) {
            throw new NullPointerException("Null type");
        }
        this.type = type;
        this.attribute = attribute;
        if (relationship == null) {
            throw new NullPointerException("Null relationship");
        }
        this.relationship = relationship;
    }

    public Label type() {
        return type;
    }

    public Statement attribute() {
        return attribute;
    }

    public Statement relationship() {
        return relationship;
    }

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
    public boolean isUnique() {
        return false;
    }

    @Override
    public Stream<Statement> getTypes() {
        return Stream.of(label(type()));
    }

    @Override
    public Stream<Statement> innerStatements() {
        return Stream.of(attribute(), relationship());
    }

    private boolean hasReifiedRelationship() {
        return relationship().getProperties().findAny().isPresent() || relationship().var().isUserDefinedName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Variable start) {
        Label type = type();
        Label has = Schema.ImplicitType.HAS.getLabel(type);
        Label key = Schema.ImplicitType.KEY.getLabel(type);

        Label hasOwnerRole = Schema.ImplicitType.HAS_OWNER.getLabel(type);
        Label keyOwnerRole = Schema.ImplicitType.KEY_OWNER.getLabel(type);
        Label hasValueRole = Schema.ImplicitType.HAS_VALUE.getLabel(type);
        Label keyValueRole = Schema.ImplicitType.KEY_VALUE.getLabel(type);

        Variable edge1 = Pattern.var();
        Variable edge2 = Pattern.var();

        return ImmutableSet.of(
                //owner rolePlayer edge
                rolePlayer(this, relationship().var(), edge1, start, null, ImmutableSet.of(hasOwnerRole, keyOwnerRole), ImmutableSet.of(has, key)),
                //value rolePlayer edge
                rolePlayer(this, relationship().var(), edge2, attribute().var(), null, ImmutableSet.of(hasValueRole, keyValueRole), ImmutableSet.of(has, key)),
                neq(this, edge1, edge2)
        );
    }

    @Override
    public Collection<PropertyExecutor> insert(Variable var) throws GraqlQueryException {
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HasAttributeProperty that = (HasAttributeProperty) o;

        if (!type().equals(that.type())) return false;
        if (!attribute().equals(that.attribute())) return false;

        // TODO: Having to check this is pretty dodgy
        // This check is necessary for `equals` and `hashCode` because `Statement` equality is defined
        // s.t. `var() != var()`, but `var().label("movie") == var().label("movie")`
        // i.e., a `Var` is compared by name, but a `Statement` ignores the name if the var is not user-defined
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
}
