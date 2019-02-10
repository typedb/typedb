package grakn.core.graql.query.pattern.statement;

import grakn.core.graql.query.pattern.property.VarProperty;

import javax.annotation.CheckReturnValue;
import java.util.LinkedHashSet;

/**
 * A Graql statement describe a Attribute
 */
public class StatementAttribute extends StatementInstance {

    private StatementAttribute(Statement statement) {
        this(statement.var(), statement.properties());
    }

    StatementAttribute(Variable var, LinkedHashSet<VarProperty> properties) {
        super(var, properties);
    }

    public static grakn.core.graql.query.pattern.statement.StatementAttribute create(Statement statement, VarProperty varProperty) {
        if (statement instanceof grakn.core.graql.query.pattern.statement.StatementAttribute) {
            return ((grakn.core.graql.query.pattern.statement.StatementAttribute) statement).addProperty(varProperty);

        } else if (!(statement instanceof StatementRelation)
                && !(statement instanceof StatementType)) {
            return new grakn.core.graql.query.pattern.statement.StatementAttribute(statement).addProperty(varProperty);

        } else {
            throw illegalArgumentException(statement, varProperty);
        }
    }

    @CheckReturnValue
    private grakn.core.graql.query.pattern.statement.StatementAttribute addProperty(VarProperty property) {
        validateNoConflictOrThrow(property);
        LinkedHashSet<VarProperty> newProperties = new LinkedHashSet<>(this.properties());
        newProperties.add(property);
        return new grakn.core.graql.query.pattern.statement.StatementAttribute(this.var(), newProperties);
    }
}
