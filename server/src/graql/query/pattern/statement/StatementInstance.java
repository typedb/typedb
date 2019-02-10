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
import grakn.core.graql.query.pattern.property.IsaProperty;
import grakn.core.graql.query.pattern.property.RelationProperty;
import grakn.core.graql.query.pattern.property.TypeProperty;
import grakn.core.graql.query.pattern.property.ValueProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public abstract class StatementInstance extends Statement {

    StatementInstance(Variable var, LinkedHashSet<VarProperty> properties) {
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

    static IllegalArgumentException illegalArgumentException(Statement statement, VarProperty varProperty) {
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

}
