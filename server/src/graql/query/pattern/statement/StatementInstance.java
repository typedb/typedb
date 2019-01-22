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

package grakn.core.graql.query.pattern.statement;

import grakn.core.graql.query.Query;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.IdProperty;
import grakn.core.graql.query.pattern.property.IsaProperty;
import grakn.core.graql.query.pattern.property.NeqProperty;
import grakn.core.graql.query.pattern.property.RelationProperty;
import grakn.core.graql.query.pattern.property.TypeProperty;
import grakn.core.graql.query.pattern.property.ValueProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public abstract class StatementInstance extends Statement {

    private StatementInstance(Variable var, LinkedHashSet<VarProperty> properties) {
        super(var, properties);
    }

    void validateRecursion() {
        Collection<Statement> toValidate = innerStatements();
        toValidate.remove(this);

        // TODO: We shouldn't be doing this to begin with if the data structure is modeled strictly
        getProperties(HasAttributeProperty.class)
                .map(property -> property.attribute())
                .flatMap(attribute -> attribute.innerStatements().stream())
                .forEach(statement -> toValidate.remove(statement));

        if (toValidate.stream()
                .anyMatch(statement -> statement.properties().stream()
                        .anyMatch(p -> !(p instanceof TypeProperty)))) {
            throw new IllegalArgumentException("The query contains nested variable properties which are not supported in native Graql");
        }
    }

    public static StatementInstance create(Statement statement, VarProperty varProperty) {
        if (statement instanceof StatementThing) {
            return StatementThing.create(statement, varProperty);

        } else if (statement instanceof StatementRelation) {
            return StatementRelation.create(statement, varProperty);

        } else if (statement instanceof StatementAttribute) {
            return StatementAttribute.create(statement, varProperty);

        } else if (!(statement instanceof StatementType)) {
            return StatementThing.create(statement, varProperty);

        } else { //if (statement instanceof StatementType)
            throw illegalArgumentException(statement, varProperty);
        }
    }

    private static IllegalArgumentException illegalArgumentException(Statement statement, VarProperty varProperty) {
        String message = "Not allowed to provide Statement Property: [" + varProperty.toString() + "] ";
        message += "to " + statement.getClass().getSimpleName() + ": [" + statement.toString() + "]";
        throw new IllegalArgumentException(message);
    }

    String isaSyntax() {
        if (getProperty(IsaProperty.class).isPresent()) {
            return getProperty(IsaProperty.class).get().toString();

        } else {
            return "";
        }
    }

    String hasSyntax() {
        return this.properties().stream()
                .filter(p -> p instanceof HasAttributeProperty)
                .map(VarProperty::toString)
                .collect(joining(Query.Char.COMMA_SPACE.toString()));
    }

    @Override
    public String toString() {
        validateRecursion();

        StringBuilder statement = new StringBuilder();

        if (this.var().isVisible()) {
            statement.append(this.var()).append(Query.Char.SPACE);
        }
        getProperty(RelationProperty.class).ifPresent(statement::append);
        getProperty(ValueProperty.class).ifPresent(statement::append);

        String properties = Stream.of(isaSyntax(), hasSyntax()).filter(s -> !s.isEmpty())
                .collect(joining(Query.Char.COMMA_SPACE.toString()));

        if (!properties.isEmpty()) {
            statement.append(Query.Char.SPACE).append(properties);
        }
        statement.append(Query.Char.SEMICOLON);
        return statement.toString();
    }

    /**
     * A Graql statement describe a Thing, which is the super type of an Entity,
     * Relation and Attribute
     */
    public static class StatementThing extends StatementInstance {

        public StatementThing(Variable var) {
            this(var, new LinkedHashSet<>());
        }

        private StatementThing(Statement statement) {
            this(statement.var(), statement.properties());
        }

        StatementThing(Variable var, LinkedHashSet<VarProperty> properties) {
            super(var, properties);
        }

        public static StatementThing create(Statement statement, VarProperty varProperty) {
            if (statement instanceof StatementThing) {
                return ((StatementThing) statement).addProperty(varProperty);

            } else if (!(statement instanceof StatementInstance)
                    && !(statement instanceof StatementType)) {
                return new StatementThing(statement).addProperty(varProperty);

            } else {
                throw illegalArgumentException(statement, varProperty);
            }
        }

        @CheckReturnValue
        StatementThing addProperty(VarProperty property) {
            validateNoConflictOrThrow(property);
            LinkedHashSet<VarProperty> newProperties = new LinkedHashSet<>(this.properties());
            newProperties.add(property);
            return new StatementThing(this.var(), newProperties);
        }

        private String thingSyntax() {
            if (!isaSyntax().isEmpty()) {
                return isaSyntax();
            } else if (getProperty(IdProperty.class).isPresent()) {
                return getProperty(IdProperty.class).get().toString();

            } else if (getProperty(NeqProperty.class).isPresent()) {
                return getProperty(NeqProperty.class).get().toString();

            } else {
                return "";
            }
        }

        @Override
        public String toString() {
            validateRecursion();

            StringBuilder statement = new StringBuilder();
            statement.append(this.var());

            String properties = Stream.of(thingSyntax(), hasSyntax()).filter(s -> !s.isEmpty())
                    .collect(joining(Query.Char.COMMA_SPACE.toString()));

            if (!properties.isEmpty()) {
                statement.append(Query.Char.SPACE).append(properties);
            }
            statement.append(Query.Char.SEMICOLON);
            return statement.toString();
        }
    }

    /**
     * A Graql statement describe a Relation
     */
    public static class StatementRelation extends StatementInstance {

        private StatementRelation(Statement statement) {
            this(statement.var(), statement.properties());
        }

        StatementRelation(Variable var, LinkedHashSet<VarProperty> properties) {
            super(var, properties);
        }

        private static StatementRelation createOrCast(Statement statement) {
            if (statement instanceof StatementRelation) {
                return (StatementRelation) statement;

            } else if (!(statement instanceof StatementAttribute)
                    && !(statement instanceof StatementType)) {
                return new StatementRelation(statement);

            } else {
                return null;
            }
        }

        public static StatementRelation create(Statement statement, VarProperty varProperty) {
            StatementRelation relation = createOrCast(statement);

            if (relation != null) {
                return relation.addProperty(varProperty);
            } else {
                throw illegalArgumentException(statement, varProperty);
            }
        }

        public static StatementRelation create(Statement statement, RelationProperty.RolePlayer rolePlayer) {
            StatementRelation relation = createOrCast(statement);

            if (relation != null) {
                return relation.addRolePlayer(rolePlayer);
            } else {
                throw illegalArgumentException(statement, new RelationProperty(Collections.singletonList(rolePlayer)));
            }
        }

        private StatementRelation addRolePlayer(RelationProperty.RolePlayer rolePlayer) {
            Optional<RelationProperty> oldRelationProperty = getProperty(RelationProperty.class);

            List<RelationProperty.RolePlayer> oldRolePlayers = oldRelationProperty
                    .map(RelationProperty::relationPlayers)
                    .orElse(Collections.emptyList());

            List<RelationProperty.RolePlayer> newRolePlayers = Stream
                    .concat(oldRolePlayers.stream(), Stream.of(rolePlayer))
                    .collect(Collectors.toList());

            RelationProperty newRelationProperty = new RelationProperty(newRolePlayers);

            if (oldRelationProperty.isPresent()) {
                StatementRelation statement = removeProperty(oldRelationProperty.get());
                return statement.addProperty(newRelationProperty);
            } else {
                return addProperty(newRelationProperty);
            }
        }

        @CheckReturnValue
        StatementRelation addProperty(VarProperty property) {
            validateNoConflictOrThrow(property);
            LinkedHashSet<VarProperty> newProperties = new LinkedHashSet<>(this.properties());
            newProperties.add(property);
            return new StatementRelation(this.var(), newProperties);
        }

        private StatementRelation removeProperty(VarProperty property) {
            Variable name = var();
            LinkedHashSet<VarProperty> newProperties = new LinkedHashSet<>(this.properties());
            newProperties.remove(property);
            return new StatementRelation(name, newProperties);
        }
    }

    /**
     * A Graql statement describe a Attribute
     */
    public static class StatementAttribute extends StatementInstance {

        private StatementAttribute(Statement statement) {
            this(statement.var(), statement.properties());
        }

        StatementAttribute(Variable var, LinkedHashSet<VarProperty> properties) {
            super(var, properties);
        }

        public static StatementAttribute create(Statement statement, VarProperty varProperty) {
            if (statement instanceof StatementAttribute) {
                return ((StatementAttribute) statement).addProperty(varProperty);

            } else if (!(statement instanceof StatementRelation)
                    && !(statement instanceof StatementType)) {
                return new StatementAttribute(statement).addProperty(varProperty);

            } else {
                throw illegalArgumentException(statement, varProperty);
            }
        }

        @CheckReturnValue
        StatementAttribute addProperty(VarProperty property) {
            validateNoConflictOrThrow(property);
            LinkedHashSet<VarProperty> newProperties = new LinkedHashSet<>(this.properties());
            newProperties.add(property);
            return new StatementAttribute(this.var(), newProperties);
        }
    }
}
