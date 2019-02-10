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

import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.property.RelationProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import javax.annotation.CheckReturnValue;

interface StatementRelationBuilder {

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param player a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    default StatementRelation rel(String player) {
        return rel(Graql.var(player));
    }

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param player a variable pattern representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    default StatementRelation rel(Statement player) {
        return statementRelation(new RelationProperty.RolePlayer(null, player));
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given Role
     *
     * @param role       a Role in the schema
     * @param player a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    default StatementRelation rel(String role, String player) {
        return rel(Graql.type(role), Graql.var(player));
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given Role
     *
     * @param role       a variable pattern representing a Role
     * @param player a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    default StatementRelation rel(Statement role, String player) {
        return rel(role, Graql.var(player));
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given Role
     *
     * @param role       a Role in the schema
     * @param player a variable pattern representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    default StatementRelation rel(String role, Statement player) {
        return rel(Graql.type(role), player);
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given Role
     *
     * @param role       a variable pattern representing a Role
     * @param player a variable pattern representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    default StatementRelation rel(Statement role, Statement player) {
        return statementRelation(new RelationProperty.RolePlayer(role, player));
    }

    @CheckReturnValue
    default StatementRelation rel(RelationProperty property) {
        return statementRelation(property);
    }

    @Deprecated         // This method should not be used publicly
    @CheckReturnValue   // TODO: will be made "private" once we upgrade to Java 9
    StatementRelation statementRelation(RelationProperty.RolePlayer rolePlayer);

    @Deprecated         // This method should not be used publicly
    @CheckReturnValue   // TODO: will be made "private" once we upgrade to Java 9
    StatementRelation statementRelation(VarProperty property);
}
