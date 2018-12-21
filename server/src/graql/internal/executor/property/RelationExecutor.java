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

package grakn.core.graql.internal.executor.property;

import grakn.core.graql.concept.Relation;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.RelationProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RelationExecutor implements PropertyExecutor.Insertable {

    private final Variable var;
    private final RelationProperty property;

    public RelationExecutor(Variable var, RelationProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.Writer> insertExecutors() {
        return Collections.unmodifiableSet(Collections.singleton(new InsertRelation()));
    }

    class InsertRelation implements PropertyExecutor.Writer {

        @Override
        public Variable var() {
            return var;
        }

        @Override
        public VarProperty property() {
            return property;
        }

        @Override
        public Set<Variable> requiredVars() {
            Set<Variable> relationPlayers = property.relationPlayers().stream()
                    .flatMap(relationPlayer -> Stream.of(relationPlayer.getPlayer(), getRole(relationPlayer)))
                    .map(statement -> statement.var())
                    .collect(Collectors.toSet());

            relationPlayers.add(var);

            return Collections.unmodifiableSet(relationPlayers);
        }

        @Override
        public Set<Variable> producedVars() {
            return Collections.unmodifiableSet(Collections.emptySet());
        }

        @Override
        public void execute(WriteExecutor executor) {
            Relation relation = executor.getConcept(var).asRelation();
            property.relationPlayers().forEach(relationPlayer -> {
                Statement roleVar = getRole(relationPlayer);

                Role role = executor.getConcept(roleVar.var()).asRole();
                Thing roleplayer = executor.getConcept(relationPlayer.getPlayer().var()).asThing();
                relation.assign(role, roleplayer);
            });
        }

        private Statement getRole(RelationProperty.RolePlayer relationPlayer) {
            return relationPlayer.getRole().orElseThrow(GraqlQueryException::insertRolePlayerWithoutRoleType);
        }
    }
}
