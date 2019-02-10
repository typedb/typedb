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

package grakn.core.graql.query.pattern.property;

import grakn.core.graql.query.Query;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.graql.query.pattern.statement.StatementRelation;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * Represents the relation property (e.g. {@code ($x, $y)} or {@code (wife: $x, husband: $y)}) on a relationship.
 * This property can be queried and inserted.
 * This propert is comprised of instances of RolePlayer, which represents associations between a
 * role-player Thing and an optional Role.
 */
public class RelationProperty extends VarProperty {

    private final List<RolePlayer> relationPlayers;

    public RelationProperty(List<RolePlayer> relationPlayers) {
        if (relationPlayers == null) {
            throw new NullPointerException("Null relationPlayers");
        }
        this.relationPlayers = relationPlayers;
    }

    public List<RolePlayer> relationPlayers() {
        return relationPlayers;
    }

    @Override
    public String keyword() {
        return Query.Property.RELATION.toString();
    }

    public String property() {
        return Query.Char.PARAN_OPEN +
                relationPlayers().stream().map(Object::toString)
                .collect(joining(Query.Char.COMMA_SPACE.toString())) +
                Query.Char.PARAN_CLOSE;
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public String toString() {
        return property();
    }

    @Override
    public Stream<Statement> types() {
        return relationPlayers().stream().map(RolePlayer::getRole)
                .flatMap(optional -> optional.map(Stream::of).orElseGet(Stream::empty));
    }

    @Override
    public Stream<Statement> statements() {
        return relationPlayers().stream().flatMap(relationPlayer -> {
            Stream.Builder<Statement> stream = Stream.builder();
            stream.add(relationPlayer.getPlayer());
            relationPlayer.getRole().ifPresent(stream::add);
            return stream.build();
        });
    }

    @Override
    public Class statementClass() {
        return StatementRelation.class;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof RelationProperty) {
            RelationProperty that = (RelationProperty) o;
            return (this.relationPlayers.equals(that.relationPlayers()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.relationPlayers.hashCode();
        return h;
    }

    /**
     * A pair of role and role player (where the role may not be present)
     */
    public static class RolePlayer {

        private final Statement role;
        private final Statement player;

        public RolePlayer(@Nullable Statement role, Statement player) {
            this.role = role;
            if (player == null) {
                throw new NullPointerException("Null player");
            }
            this.player = player;
        }

        /**
         * @return the role, if specified
         */
        @CheckReturnValue
        public Optional<Statement> getRole() {
            return Optional.ofNullable(role);
        }

        /**
         * @return the role player
         */
        @CheckReturnValue
        public Statement getPlayer() {
            return player;
        }

        @Override
        public String toString() {
            return getRole().map(r -> r.getPrintableName() + ": ").orElse("") + getPlayer().getPrintableName();
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof RolePlayer) {
                RolePlayer that = (RolePlayer) o;
                return (Objects.equals(this.role, that.role))
                        && (this.player.equals(that.player));
            }
            return false;
        }

        @Override
        public int hashCode() {
            int h = 1;
            h *= 1000003;
            if (this.role != null) {
                h ^= this.role.hashCode();
            }
            h *= 1000003;
            h ^= this.player.hashCode();
            return h;
        }
    }
}
