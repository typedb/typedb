/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.pattern.constraint.thing;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalence;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalent;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.pattern.variable.VariableCloner;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.COLON;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.COMMA;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.PARAN_CLOSE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.PARAN_OPEN;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;

public class RelationConstraint extends ThingConstraint implements AlphaEquivalent<RelationConstraint> {

    private final LinkedHashSet<RolePlayer> rolePlayers;
    private final int hash;

    public RelationConstraint(ThingVariable owner, LinkedHashSet<RolePlayer> rolePlayers) {
        super(owner, rolePlayerVariables(rolePlayers));
        assert !rolePlayers.isEmpty();
        this.rolePlayers = new LinkedHashSet<>(rolePlayers);
        this.hash = Objects.hash(RelationConstraint.class, this.owner, this.rolePlayers);
        for (RelationConstraint.RolePlayer rp : rolePlayers) {
            rp.player().constraining(this);
            rp.roleType().ifPresent(roleType -> roleType.constraining(this));
        }
    }

    static RelationConstraint of(ThingVariable owner, com.vaticle.typeql.lang.pattern.constraint.ThingConstraint.Relation constraint,
                                 VariableRegistry register) {
        LinkedHashSet<RolePlayer> rolePlayers = new LinkedHashSet<>();
        iterate(constraint.players()).map(rp -> RolePlayer.of(rp, register)).toSet(rolePlayers);
        return new RelationConstraint(owner, rolePlayers);
    }

    static RelationConstraint of(ThingVariable owner, RelationConstraint clone, VariableCloner cloner) {
        LinkedHashSet<RolePlayer> rolePlayers = new LinkedHashSet<>();
        iterate(clone.players()).map(rp -> RolePlayer.of(rp, cloner)).toSet(rolePlayers);
        return new RelationConstraint(owner, rolePlayers);
    }

    @Override
    public RelationConstraint clone(Conjunction.ConstraintCloner cloner) {
        LinkedHashSet<RolePlayer> clonedRPs = new LinkedHashSet<>();
        iterate(this.rolePlayers).map(rolePlayer -> rolePlayer.clone(cloner)).toSet(clonedRPs);
        return cloner.cloneVariable(owner()).relation(clonedRPs);
    }

    public LinkedHashSet<RolePlayer> players() {
        return rolePlayers;
    }

    @Override
    public void addTo(GraphTraversal.Thing traversal) {
        for (RolePlayer rolePlayer : rolePlayers) {
            assert !rolePlayer.inferredRoleTypes.isEmpty();
            ThingVariable player = rolePlayer.player();
            int rep = rolePlayer.repetition();
            if (rolePlayer.roleType().isPresent() && rolePlayer.roleType().get().id().isName()) {
                Identifier.Scoped role = Identifier.Scoped.of(owner.id(), rolePlayer.roleType().get().id(), player.id(), rep);
                traversal.relating(owner.id(), role);
                traversal.playing(player.id(), role);
                traversal.isa(role, rolePlayer.roleType().get().id());
                traversal.types(role, rolePlayer.inferredRoleTypes());
            } else if (rep > 1) {
                Identifier.Scoped role = Identifier.Scoped.of(owner.id(), null, player.id(), rep);
                traversal.relating(owner.id(), role);
                traversal.playing(player.id(), role);
                traversal.types(role, rolePlayer.inferredRoleTypes());
            } else {
                traversal.rolePlayer(owner.id(), player.id(), rolePlayer.inferredRoleTypes(), rep);
            }
        }
    }

    @Override
    public boolean isRelation() {
        return true;
    }

    @Override
    public RelationConstraint asRelation() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationConstraint that = (RelationConstraint) o;
        return (this.owner.equals(that.owner) && this.rolePlayers.equals(that.rolePlayers));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    private static Set<Variable> rolePlayerVariables(Set<RolePlayer> rolePlayers) {
        Set<com.vaticle.typedb.core.pattern.variable.Variable> variables = new HashSet<>();
        rolePlayers.forEach(player -> {
            variables.add(player.player());
            if (player.roleType().isPresent()) variables.add(player.roleType().get());
        });
        return variables;
    }

    @Override
    public FunctionalIterator<AlphaEquivalence> alphaEquals(RelationConstraint that) {
        return owner.alphaEquals(that.owner)
                .flatMap(a -> a.alphaEqualIf(players().size() == that.players().size()))
                .flatMap(a -> roleplayerEquivalences(that).flatMap(a::extendIfCompatible));
    }

    private FunctionalIterator<AlphaEquivalence> roleplayerEquivalences(RelationConstraint that) {
        return Iterators.permutation(players()).flatMap(playersPermutation -> {
            Iterator<RolePlayer> thisRolePlayers = playersPermutation.iterator();
            Iterator<RolePlayer> thatRolePlayers = that.players().iterator();
            AlphaEquivalence permutationMap = AlphaEquivalence.empty();
            while (thisRolePlayers.hasNext() && thatRolePlayers.hasNext()) {
                permutationMap = thisRolePlayers.next().alphaEquals(thatRolePlayers.next())
                        .flatMap(permutationMap::extendIfCompatible).firstOrNull();
                if (permutationMap == null ) return Iterators.empty();
            }
            return Iterators.single(permutationMap);
        }).distinct();
    }

    public static class RolePlayer implements AlphaEquivalent<RolePlayer> {

        private final TypeVariable roleType;
        private final ThingVariable player;
        private final int repetition;
        private final int hash;
        private final Set<Label> inferredRoleTypes;

        public RolePlayer(@Nullable TypeVariable roleType, ThingVariable player, int repetition) {
            assert roleType == null || roleType.id().isName() ||
                    (roleType.label().isPresent() && roleType.label().get().scope().isPresent());
            if (player == null) throw new NullPointerException("Null player");
            this.roleType = roleType;
            this.player = player;
            this.inferredRoleTypes = new HashSet<>();
            this.repetition = repetition;
            this.hash = Objects.hash(this.roleType, this.player, this.repetition);
        }

        public static RolePlayer of(com.vaticle.typeql.lang.pattern.constraint.ThingConstraint.Relation.RolePlayer constraint,
                                    VariableRegistry registry) {
            return new RolePlayer(
                    constraint.roleType().map(registry::register).orElse(null),
                    registry.register(constraint.player()),
                    constraint.repetition()
            );
        }

        public static RolePlayer of(RolePlayer clone, VariableCloner cloner) {
            RolePlayer rolePlayer = new RolePlayer(
                    clone.roleType().map(cloner::clone).orElse(null),
                    cloner.clone(clone.player()),
                    clone.repetition()
            );
            rolePlayer.setInferredRoleTypes(clone.inferredRoleTypes);
            return rolePlayer;
        }

        public int repetition() {
            return repetition;
        }

        public Optional<TypeVariable> roleType() {
            return Optional.ofNullable(roleType);
        }

        public Set<Label> inferredRoleTypes() {
            return inferredRoleTypes;
        }

        public void setInferredRoleTypes(Set<Label> roleTypes) {
            this.inferredRoleTypes.clear();
            this.inferredRoleTypes.addAll(roleTypes);
        }

        public ThingVariable player() {
            return player;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RolePlayer that = (RolePlayer) o;
            return (Objects.equals(this.roleType, that.roleType) &&
                    this.player.equals(that.player) &&
                    this.repetition == that.repetition);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            return (roleType != null ? roleType.toString() + COLON : "") + player.toString();
        }

        @Override
        public FunctionalIterator<AlphaEquivalence> alphaEquals(RolePlayer that) {
            return AlphaEquivalence.alphaEquals(roleType, that.roleType)
                    .flatMap(a -> player.alphaEquals(that.player).flatMap(a::extendIfCompatible));
        }

        public RolePlayer clone(Conjunction.ConstraintCloner cloner) {
            TypeVariable roleTypeClone = roleType == null ? null : cloner.cloneVariable(roleType);
            ThingVariable playerClone = cloner.cloneVariable(player);
            RolePlayer rpClone = new RolePlayer(roleTypeClone, playerClone, repetition);
            rpClone.setInferredRoleTypes(this.inferredRoleTypes);
            return rpClone;
        }
    }

    @Override
    public String toString() {
        return owner.toString() + SPACE + PARAN_OPEN
                + rolePlayers.stream().map(RolePlayer::toString).collect(Collectors.joining("" + COMMA + SPACE))
                + PARAN_CLOSE;
    }
}
