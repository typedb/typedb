package grakn.core.graql.query.pattern.statement;

import grakn.core.graql.query.Query;
import grakn.core.graql.query.pattern.property.IdProperty;
import grakn.core.graql.query.pattern.property.NeqProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

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

    public static grakn.core.graql.query.pattern.statement.StatementThing create(Statement statement, VarProperty varProperty) {
        if (statement instanceof grakn.core.graql.query.pattern.statement.StatementThing) {
            return ((grakn.core.graql.query.pattern.statement.StatementThing) statement).addProperty(varProperty);

        } else if (!(statement instanceof StatementInstance)
                && !(statement instanceof StatementType)) {
            return new grakn.core.graql.query.pattern.statement.StatementThing(statement).addProperty(varProperty);

        } else {
            throw illegalArgumentException(statement, varProperty);
        }
    }

    @CheckReturnValue
    private grakn.core.graql.query.pattern.statement.StatementThing addProperty(VarProperty property) {
        validateNoConflictOrThrow(property);
        LinkedHashSet<VarProperty> newProperties = new LinkedHashSet<>(this.properties());
        newProperties.add(property);
        return new grakn.core.graql.query.pattern.statement.StatementThing(this.var(), newProperties);
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
