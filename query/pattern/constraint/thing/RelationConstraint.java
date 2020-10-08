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

package grakn.core.query.pattern.constraint.thing;

import grakn.core.query.pattern.variable.ThingVariable;
import grakn.core.query.pattern.variable.TypeVariable;
import grakn.core.query.pattern.variable.Variable;
import grakn.core.query.pattern.variable.VariableRegistry;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.toList;

public class RelationConstraint extends ThingConstraint {

    private final List<RolePlayer> players;
    private final int hash;

    private RelationConstraint(final ThingVariable owner, final List<RolePlayer> players) {
        super(owner);
        assert players != null && !players.isEmpty();
        this.players = new ArrayList<>(players);
        this.hash = Objects.hash(RelationConstraint.class, this.owner, this.players);
    }

    public static RelationConstraint of(final ThingVariable owner,
                                        final graql.lang.pattern.constraint.ThingConstraint.Relation constraint,
                                        final VariableRegistry register) {
        return new RelationConstraint(owner, constraint.players().stream()
                .map(rp -> RolePlayer.of(rp, register)).collect(toList()));
    }

    public List<RolePlayer> players() {
        return players;
    }

    @Override
    public Set<Variable> variables() {
        final Set<Variable> variables = new HashSet<>();
        players().forEach(player -> {
            variables.add(player.player());
            if (player.roleType().isPresent()) variables.add(player.roleType().get());
        });
        return variables;
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
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final RelationConstraint that = (RelationConstraint) o;
        return (this.owner.equals(that.owner) && this.players.equals(that.players));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public static class RolePlayer {

        private final TypeVariable roleType;
        private final ThingVariable player;
        private final int hash;

        private RolePlayer(@Nullable final TypeVariable roleType, final ThingVariable player) {
            if (player == null) throw new NullPointerException("Null player");
            this.roleType = roleType;
            this.player = player;
            this.hash = Objects.hash(this.roleType, this.player);
        }

        public static RolePlayer of(final graql.lang.pattern.constraint.ThingConstraint.Relation.RolePlayer constraint,
                                    final VariableRegistry registry) {
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

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final RolePlayer that = (RolePlayer) o;
            return (Objects.equals(this.roleType, that.roleType)) && (this.player.equals(that.player));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
