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

package grakn.core.pattern.constraint.thing;

import grakn.core.common.iterator.Iterators;
import grakn.core.common.parameters.Label;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.pattern.equivalence.AlphaEquivalent;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.Identifier;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static graql.lang.common.GraqlToken.Char.PARAN_CLOSE;
import static graql.lang.common.GraqlToken.Char.PARAN_OPEN;
import static java.util.stream.Collectors.toList;

public class RelationConstraint extends ThingConstraint implements AlphaEquivalent<RelationConstraint> {

    private final List<RolePlayer> rolePlayers;
    private final int hash;

    public RelationConstraint(ThingVariable owner, List<RolePlayer> rolePlayers) {
        super(owner, rolePlayerVariables(rolePlayers));
        assert rolePlayers != null && !rolePlayers.isEmpty();
        this.rolePlayers = new ArrayList<>(rolePlayers);
        this.hash = Objects.hash(RelationConstraint.class, this.owner, this.rolePlayers);
    }

    static RelationConstraint of(ThingVariable owner, graql.lang.pattern.constraint.ThingConstraint.Relation constraint,
                                 VariableRegistry register) {
        return new RelationConstraint(owner, constraint.players().stream()
                .map(rp -> RolePlayer.of(rp, register)).collect(toList()));
    }

    public List<RolePlayer> players() {
        return rolePlayers;
    }

    @Override
    public void addTo(Traversal traversal) {
        rolePlayers.forEach(rp -> {
            if (rp.roleType().isPresent()) {
                if (rp.roleType().get().reference().isName() || rp.roleTypeHints().isEmpty()) {
                    Identifier.Scoped role = traversal.newIdentifier(owner.identifier());
                    traversal.relating(owner.identifier(), role);
                    traversal.playing(rp.player().identifier(), role);
                    traversal.isa(role, rp.roleType().get().identifier());
                    if (!rp.roleTypeHints.isEmpty()) traversal.types(role, rp.roleTypeHints);
                } else {
                    assert rp.roleType().get().reference().isLabel() && !rp.roleTypeHints.isEmpty();
                    traversal.rolePlayer(owner.identifier(), rp.player().identifier(), rp.roleTypeHints);
                }
            } else {
                traversal.rolePlayer(owner.identifier(), rp.player().identifier());
            }
        });
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
        final RelationConstraint that = (RelationConstraint) o;
        return (this.owner.equals(that.owner) && this.rolePlayers.equals(that.rolePlayers));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    private static Set<Variable> rolePlayerVariables(List<RolePlayer> rolePlayers) {
        final Set<grakn.core.pattern.variable.Variable> variables = new HashSet<>();
        rolePlayers.forEach(player -> {
            variables.add(player.player());
            if (player.roleType().isPresent()) variables.add(player.roleType().get());
        });
        return variables;
    }

    @Override
    public AlphaEquivalence alphaEquals(RelationConstraint that) {
        return AlphaEquivalence.valid()
                .validIf(players().size() == that.players().size())
                .addOrInvalidate(() -> Iterators.permutation(players()).stream().map(playersPermutation -> {
                    Iterator<RolePlayer> thisRolePlayersIt = playersPermutation.iterator();
                    Iterator<RolePlayer> thatRolePlayersIt = that.players().iterator();
                    AlphaEquivalence permutationMap = AlphaEquivalence.valid();
                    while (thisRolePlayersIt.hasNext() && thatRolePlayersIt.hasNext()) {
                        permutationMap = permutationMap.validIfAlphaEqual(thisRolePlayersIt.next(), thatRolePlayersIt.next());
                        if (!permutationMap.isValid()) {
                            return permutationMap;
                        }
                    }
                    return permutationMap;
                }).filter(AlphaEquivalence::isValid).findFirst().orElse(AlphaEquivalence.invalid()));
    }

    public static class RolePlayer implements AlphaEquivalent<RolePlayer> {

        private final TypeVariable roleType;
        private final ThingVariable player;
        private final int hash;
        private Set<Label> roleTypeHints;

        public RolePlayer(@Nullable TypeVariable roleType, ThingVariable player) {
            if (player == null) throw new NullPointerException("Null player");
            this.roleType = roleType;
            this.player = player;
            this.hash = Objects.hash(this.roleType, this.player);
            this.roleTypeHints = new HashSet<>();
        }

        public static RolePlayer of(graql.lang.pattern.constraint.ThingConstraint.Relation.RolePlayer constraint,
                                    VariableRegistry registry) {
            return new RolePlayer(
                    constraint.roleType().map(registry::register).orElse(null),
                    registry.register(constraint.player())
            );
        }

        public Optional<TypeVariable> roleType() {
            return Optional.ofNullable(roleType);
        }

        public ThingVariable player() {
            return player;
        }

        public void addRoleTypeHints(Set<Label> labels) {
            this.roleTypeHints = labels;
        }

        public Set<Label> roleTypeHints() {
            return roleTypeHints;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final RolePlayer that = (RolePlayer) o;
            return (Objects.equals(this.roleType, that.roleType)) && (this.player.equals(that.player));
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            return (roleType == null ? "" : roleType.referenceSyntax() + ":") + player.reference().toString();
        }

        @Override
        public AlphaEquivalence alphaEquals(RolePlayer that) {
            return AlphaEquivalence.valid()
                    .validIf(roleTypeHints.equals(that.roleTypeHints))
                    .validIfAlphaEqual(roleType, that.roleType)
                    .validIfAlphaEqual(player, that.player);
        }
    }

    @Override
    public String toString() {
        return rolePlayers.stream().map(RolePlayer::toString)
                .collect(Collectors.joining(", ", PARAN_OPEN.toString(), PARAN_CLOSE.toString()));
    }
}
