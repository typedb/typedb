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
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
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
        public TiebreakDeletionOrdering ordering(WriteExecutor executor) {
            return TiebreakDeletionOrdering.EDGE;
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
            return allVars();

        }

        @Override
        public Set<Variable> producedVars() {
            return Collections.emptySet();
        }

        private Set<Variable> allVars() {
            Set<Variable> relationPlayers = property.relationPlayers().stream()
                    .flatMap(relationPlayer -> Stream.of(relationPlayer.getPlayer().var(), getRoleVar(relationPlayer)))
                    .collect(Collectors.toSet());

            relationPlayers.add(var);
            return Collections.unmodifiableSet(relationPlayers);
        }

        @Override
        public void execute(WriteExecutor executor) {
            if (!executor.getConcept(var).isRelation()) {
                throw GraqlSemanticException.notARelationInstance(var, executor.getConcept(var));
            }

            Relation relation = executor.getConcept(var).asRelation();

            /*
            As of Grakn 2.0, with Hypergraph backend, we will no longer allow a relation relate both a role,
            AND any of its supertypes, because roles will either be inherited OR overriden with the `as` keyword.
            So, when we have queries like:

            ```
            match $r (sub-role: $x) isa relation;
            delete $r (super-role: $x);
            ```

            Then this is no longer ambiguous: the relation $r can only have 1 "subtype" of `super-role` in roles played

            In prior versions, this delete could be ambigous in the case where the following could be written:
            ```
            match $r (sub-role: $x, super-role: $x) isa relation;
            delete $r (super-role: $x)
            ```
            in this example, we don't know which one to delete - the sub-role role player or the super-role player
             */
            property.relationPlayers().forEach(relationPlayer -> {
                Role requiredRole = getRole(relationPlayer, executor);
                Variable rolePlayerVar = relationPlayer.getPlayer().var();
                Thing rolePlayer = executor.getConcept(rolePlayerVar).asThing();

                // find the first role subtype that is the actual role being played
                Optional<Role> concreteRolePlayed = requiredRole.subs()
                        .filter(role -> relation.rolePlayers(role).anyMatch(rolePlayer::equals))
                        .findFirst();

                if (!concreteRolePlayed.isPresent()) {
                    throw GraqlSemanticException.cannotDeleteRPNoCompatiblePlayer(rolePlayerVar, rolePlayer, var, relation, requiredRole.label());
               }

                relation.unassign(concreteRolePlayed.get(), rolePlayer);
            });
        }

        private Role getRole(RelationProperty.RolePlayer relationPlayer, WriteExecutor executor) {
            boolean roleExists = relationPlayer.getRole().isPresent();
            if (!roleExists) {
                throw GraqlSemanticException.deleteRolePlayerWithoutRoleType(relationPlayer.toString());
            } else {
                Statement roleStatement = relationPlayer.getRole().get();
                Variable roleVar = roleStatement.var();
                if (roleStatement.getType().isPresent()) {
                    executor.getBuilder(roleVar).label(Label.of(roleStatement.getType().get()));
                }
                return executor.getConcept(roleVar).asRole();
            }
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
