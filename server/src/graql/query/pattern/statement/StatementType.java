package grakn.core.graql.query.pattern.statement;

import grakn.core.graql.query.Query;
import grakn.core.graql.query.pattern.property.TypeProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.LinkedHashSet;

import static java.util.stream.Collectors.joining;

public class StatementType extends Statement {

    private StatementType(Statement statement) {
        this(statement.var(), statement.properties(), statement.sign());
    }

    private StatementType(Variable var, LinkedHashSet<VarProperty> properties, Sign sign) {
        super(var, properties, sign);
    }

    public static StatementType create(Statement statement, VarProperty varProperty) {
        if (statement instanceof StatementType) {
            return ((StatementType) statement).addProperty(varProperty);

        } else if (statement instanceof StatementInstance) {
            String message = "Not allowed to provide Type Statement Property: [" + varProperty.toString() + "] ";
            message += "to Instance Statement: [" + statement.toString() + "]";
            throw new IllegalArgumentException(message);

        } else {
            return new StatementType(statement).addProperty(varProperty);
        }
    }

    @CheckReturnValue
    private StatementType addProperty(VarProperty property) {
        validateNonUniqueOrThrow(property);
        LinkedHashSet<VarProperty> newProperties = new LinkedHashSet<>(this.properties());
        newProperties.add(property);
        return new StatementType(this.var(), newProperties, this.sign());
    }

    @Override @SuppressWarnings("Duplicates")
    public String toString() {
        Collection<Statement> innerStatements = innerStatements();
        innerStatements.remove(this);

        // TODO: Remove this once we make type labels to be part of a Variable
        if (innerStatements.stream()
                .anyMatch(statement -> statement.properties().stream()
                        .anyMatch(p -> !(p instanceof TypeProperty)))) {
            LOG.warn("printing a query with inner variables, which is not supported in native Graql");
        }

        StringBuilder statement = new StringBuilder();
        if (!isPositive()) {
            statement.append(Query.Operator.NOT).append(Query.Char.SPACE);
        }

        if (this.var().isVisible()) {
            statement.append(this.var()).append(Query.Char.SPACE);
            statement.append(this.properties().stream()
                                     .map(VarProperty::toString)
                                     .collect(joining(Query.Char.COMMA_SPACE.toString())));
        } else {
            statement.append(getProperty(TypeProperty.class).get().property()).append(Query.Char.SPACE);
            statement.append(this.properties().stream().filter(p -> !(p instanceof TypeProperty))
                                     .map(VarProperty::toString)
                                     .collect(joining(Query.Char.COMMA_SPACE.toString())));
        }

        statement.append(Query.Char.SEMICOLON);
        return statement.toString();
    }
}
