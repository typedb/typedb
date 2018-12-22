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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.core.graql.concept.Relation;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.RelationProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.common.util.CommonUtil.toImmutableSet;
import static grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets.rolePlayer;

public class RelationExecutor implements PropertyExecutor.Insertable, PropertyExecutor.Matchable {

    private final Variable var;
    private final RelationProperty property;

    public RelationExecutor(Variable var, RelationProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.Writer> insertExecutors() {
        return ImmutableSet.of(new InsertRelation());
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        Collection<Variable> castingNames = new HashSet<>();

        ImmutableSet<EquivalentFragmentSet> traversals = property.relationPlayers().stream().flatMap(relationPlayer -> {

            Variable castingName = Pattern.var();
            castingNames.add(castingName);

            return fragmentSetsFromRolePlayer(castingName, relationPlayer);
        }).collect(toImmutableSet());

        ImmutableSet<EquivalentFragmentSet> distinctCastingTraversals = castingNames.stream().flatMap(
                castingName -> castingNames.stream()
                        .filter(otherName -> !otherName.equals(castingName))
                        .map(otherName -> EquivalentFragmentSets.neq(property, castingName, otherName))
        ).collect(toImmutableSet());

        return Sets.union(traversals, distinctCastingTraversals);
    }

    private Stream<EquivalentFragmentSet> fragmentSetsFromRolePlayer(Variable castingName,
                                                                     RelationProperty.RolePlayer relationPlayer) {
        Optional<Statement> roleType = relationPlayer.getRole();

        if (roleType.isPresent()) {
            // Add some patterns where this variable is a relation relating the given roleplayer as the given roletype
            return Stream.of(rolePlayer(property, var, castingName, relationPlayer.getPlayer().var(), roleType.get().var()));
        } else {
            // Add some patterns where this variable is a relation and the given variable is a roleplayer of that relationship
            return Stream.of(rolePlayer(property, var, castingName, relationPlayer.getPlayer().var(), null));
        }
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
            return ImmutableSet.of();
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
