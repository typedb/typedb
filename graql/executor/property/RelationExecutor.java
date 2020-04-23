/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.executor.property;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.graql.exception.GraqlQueryException;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.executor.WriteExecutor;
import grakn.core.kb.graql.executor.property.PropertyExecutor;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import graql.lang.property.RelationProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets.rolePlayer;

public class RelationExecutor implements PropertyExecutor.Insertable, PropertyExecutor.Deletable {

    private final Variable var;
    private final RelationProperty property;

    RelationExecutor(Variable var, RelationProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        Collection<Variable> castingNames = new HashSet<>();

        ImmutableSet<EquivalentFragmentSet> traversals =
                property.relationPlayers().stream().flatMap(relationPlayer -> {
                    Variable castingName = new Variable();
                    castingNames.add(castingName);
                    return fragmentSetsFromRolePlayer(castingName, relationPlayer);
                }).collect(ImmutableSet.toImmutableSet());

        ImmutableSet<EquivalentFragmentSet> distinctCastingTraversals = castingNames.stream().flatMap(
                castingName -> castingNames.stream()
                        .filter(otherName -> !otherName.equals(castingName))
                        .map(otherName -> EquivalentFragmentSets.neq(property, castingName, otherName))
        ).collect(ImmutableSet.toImmutableSet());

        return Sets.union(traversals, distinctCastingTraversals);
    }

    private Stream<EquivalentFragmentSet> fragmentSetsFromRolePlayer(Variable castingName,
                                                                     RelationProperty.RolePlayer relationPlayer) {
        Optional<Statement> roleType = relationPlayer.getRole();

        if (roleType.isPresent()) {
            // Patterns for variable of a relation with a role player of a given type
            return Stream.of(rolePlayer(property, var, castingName,
                    relationPlayer.getPlayer().var(),
                    roleType.get().var()));
        } else {
            // Patterns for variable of a relation with a role player of a any type
            return Stream.of(rolePlayer(property, var, castingName,
                    relationPlayer.getPlayer().var(),
                    null));
        }
    }

    @Override
    public Set<PropertyExecutor.Writer> insertExecutors() {
        return ImmutableSet.of(new InsertRelation());
    }

    @Override
    public Set<Writer> deleteExecutors() {
        return ImmutableSet.of(new DeleteRelation());
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
                    .flatMap(relationPlayer -> Stream.of(relationPlayer.getPlayer().var(), getRoleVar(relationPlayer)))
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
                Variable roleVar = getRoleVar(relationPlayer);
                Role role = executor.getConcept(roleVar).asRole();
                Thing roleplayer = executor.getConcept(relationPlayer.getPlayer().var()).asThing();
                relation.assign(role, roleplayer);
            });
        }

        private Variable getRoleVar(RelationProperty.RolePlayer relationPlayer) {
            boolean roleExists = relationPlayer.getRole().isPresent();
            if (!roleExists) {
                throw GraqlSemanticException.deleteRolePlayerWithoutRoleType(relationPlayer.toString());
            } else {
                Statement role = relationPlayer.getRole().get();
                return role.var();
            }
        }
    }

    class DeleteRelation implements PropertyExecutor.Writer {

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
                    .flatMap(relationPlayer -> Stream.of(relationPlayer.getPlayer().var(), getRoleVar(relationPlayer)))
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
            if (!executor.getConcept(var).isRelation()) {
                throw GraqlQueryException.create(String.format("Expect %s [%s] to be a relation.", var, executor.getConcept(var)));
            }
            Relation relation = executor.getConcept(var).asRelation();
            property.relationPlayers().forEach(relationPlayer -> {
                Variable roleVar = getRoleVar(relationPlayer);
                Role role = executor.getConcept(roleVar).asRole();
                Variable rolePlayerVar = relationPlayer.getPlayer().var();
                Thing rolePlayer = executor.getConcept(rolePlayerVar).asThing();

                // validate that the role player plays this role in this relation
                boolean roleIsPlayed = relation.rolePlayers(role).anyMatch(rolePlayer::equals);
                if (!roleIsPlayed) {
                    // TODO better exception
                    throw GraqlQueryException.create(
                            String.format("Concept %s [%s] does not play a role %s in relation %s [%s], so cannot unassign from relation.",
                                    rolePlayer, rolePlayerVar, role, relation, var));
                }

                relation.unassign(role, rolePlayer);
            });
        }

        private Variable getRoleVar(RelationProperty.RolePlayer relationPlayer) {
            boolean roleExists = relationPlayer.getRole().isPresent();
            if (!roleExists) {
                throw GraqlSemanticException.deleteRolePlayerWithoutRoleType(relationPlayer.toString());
            } else {
                Statement role = relationPlayer.getRole().get();
                return role.var();
            }
        }
    }
}
