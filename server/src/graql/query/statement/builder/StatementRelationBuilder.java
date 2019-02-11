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

package grakn.core.graql.query.statement.builder;

import grakn.core.graql.query.Graql;
import grakn.core.graql.query.property.RelationProperty;
import grakn.core.graql.query.property.VarProperty;
import grakn.core.graql.query.statement.Statement;
import grakn.core.graql.query.statement.StatementRelation;

import javax.annotation.CheckReturnValue;

public interface StatementRelationBuilder extends StatementInstanceBuilder {

    @CheckReturnValue
    default StatementRelation rel(String player) {
        return rel(Graql.var(player));
    }

    @CheckReturnValue
    default StatementRelation rel(Statement player) {
        return relation(new RelationProperty.RolePlayer(null, player));
    }

    @CheckReturnValue
    default StatementRelation rel(String role, String player) {
        return rel(Graql.type(role), Graql.var(player));
    }

    @CheckReturnValue
    default StatementRelation rel(Statement role, String player) {
        return rel(role, Graql.var(player));
    }

    @CheckReturnValue
    default StatementRelation rel(String role, Statement player) {
        return rel(Graql.type(role), player);
    }

    @CheckReturnValue
    default StatementRelation rel(Statement role, Statement player) {
        return relation(new RelationProperty.RolePlayer(role, player));
    }

    @CheckReturnValue
    default StatementRelation rel(RelationProperty property) {
        return relation(property);
    }

    @CheckReturnValue   // TODO: will be made "private" once we upgrade to Java 9 or higher
    StatementRelation relation(RelationProperty.RolePlayer rolePlayer);

    @CheckReturnValue   // TODO: will be made "private" once we upgrade to Java 9 or higher
    StatementRelation relation(VarProperty property);
}
