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

package graql.lang.statement;

import graql.lang.property.IdProperty;
import graql.lang.property.NeqProperty;
import graql.lang.property.VarProperty;
import graql.lang.util.Token;

import javax.annotation.CheckReturnValue;
import java.util.LinkedHashSet;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * A Graql statement describe a Thing, which is the super type of an Entity,
 * Relation and Attribute
 */
public class StatementThing extends StatementInstance {

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
    private StatementThing addProperty(VarProperty property) {
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
                .collect(joining(Token.Char.COMMA_SPACE.toString()));

        if (!properties.isEmpty()) {
            statement.append(Token.Char.SPACE).append(properties);
        }
        statement.append(Token.Char.SEMICOLON);
        return statement.toString();
    }
}
