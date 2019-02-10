package grakn.core.graql.query.pattern.statement;

import grakn.core.graql.query.pattern.property.RelationProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import javax.annotation.CheckReturnValue;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Graql statement describe a Relation
 */
public class StatementRelation extends StatementInstance {

    private StatementRelation(Statement statement) {
        this(statement.var(), statement.properties());
    }

    StatementRelation(Variable var, LinkedHashSet<VarProperty> properties) {
        super(var, properties);
    }

    private static grakn.core.graql.query.pattern.statement.StatementRelation createOrCast(Statement statement) {
        if (statement instanceof grakn.core.graql.query.pattern.statement.StatementRelation) {
            return (grakn.core.graql.query.pattern.statement.StatementRelation) statement;

        } else if (!(statement instanceof StatementAttribute)
                && !(statement instanceof StatementType)) {
            return new grakn.core.graql.query.pattern.statement.StatementRelation(statement);

        } else {
            return null;
        }
    }

    public static grakn.core.graql.query.pattern.statement.StatementRelation create(Statement statement, VarProperty varProperty) {
        grakn.core.graql.query.pattern.statement.StatementRelation relation = createOrCast(statement);

        if (relation != null) {
            return relation.addProperty(varProperty);
        } else {
            throw illegalArgumentException(statement, varProperty);
        }
    }

    public static grakn.core.graql.query.pattern.statement.StatementRelation create(Statement statement, RelationProperty.RolePlayer rolePlayer) {
        grakn.core.graql.query.pattern.statement.StatementRelation relation = createOrCast(statement);

        if (relation != null) {
            return relation.addRolePlayer(rolePlayer);
        } else {
            throw illegalArgumentException(statement, new RelationProperty(Collections.singletonList(rolePlayer)));
        }
    }

    private grakn.core.graql.query.pattern.statement.StatementRelation addRolePlayer(RelationProperty.RolePlayer rolePlayer) {
        Optional<RelationProperty> oldRelationProperty = getProperty(RelationProperty.class);

        List<RelationProperty.RolePlayer> oldRolePlayers = oldRelationProperty
                .map(RelationProperty::relationPlayers)
                .orElse(Collections.emptyList());

        List<RelationProperty.RolePlayer> newRolePlayers = Stream
                .concat(oldRolePlayers.stream(), Stream.of(rolePlayer))
                .collect(Collectors.toList());

        RelationProperty newRelationProperty = new RelationProperty(newRolePlayers);

        if (oldRelationProperty.isPresent()) {
            grakn.core.graql.query.pattern.statement.StatementRelation statement = removeProperty(oldRelationProperty.get());
            return statement.addProperty(newRelationProperty);
        } else {
            return addProperty(newRelationProperty);
        }
    }

    @CheckReturnValue
    private grakn.core.graql.query.pattern.statement.StatementRelation addProperty(VarProperty property) {
        validateNoConflictOrThrow(property);
        LinkedHashSet<VarProperty> newProperties = new LinkedHashSet<>(this.properties());
        newProperties.add(property);
        return new grakn.core.graql.query.pattern.statement.StatementRelation(this.var(), newProperties);
    }

    private grakn.core.graql.query.pattern.statement.StatementRelation removeProperty(VarProperty property) {
        Variable name = var();
        LinkedHashSet<VarProperty> newProperties = new LinkedHashSet<>(this.properties());
        newProperties.remove(property);
        return new grakn.core.graql.query.pattern.statement.StatementRelation(name, newProperties);
    }
}
