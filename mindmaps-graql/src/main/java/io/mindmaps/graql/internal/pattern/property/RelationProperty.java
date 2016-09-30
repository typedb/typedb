/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.pattern.property;

import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.FragmentImpl;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.MultiTraversalImpl;
import io.mindmaps.graql.internal.gremlin.ShortcutTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.*;
import java.util.stream.Stream;

import static io.mindmaps.graql.internal.gremlin.FragmentPriority.*;
import static io.mindmaps.util.Schema.EdgeLabel.*;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

public class RelationProperty implements VarPropertyInternal {

    private final Set<VarAdmin.Casting> castings = new HashSet<>();

    public void addCasting(VarAdmin.Casting casting) {
        castings.add(casting);
    }

    public Stream<VarAdmin.Casting> getCastings() {
        return castings.stream();
    }

    @Override
    public void buildString(StringBuilder builder) {
        builder.append("(").append(castings.stream().map(Object::toString).collect(joining(", "))).append(")");
    }

    @Override
    public void modifyShortcutTraversal(ShortcutTraversal shortcutTraversal) {
        castings.forEach(casting -> {
            Optional<VarAdmin> roleType = casting.getRoleType();

            if (roleType.isPresent()) {
                Optional<String> roleTypeId = roleType.get().getIdOnly();

                if (roleTypeId.isPresent()) {
                    shortcutTraversal.addRel(roleTypeId.get(), casting.getRolePlayer().getName());
                } else {
                    shortcutTraversal.setInvalid();
                }

            } else {
                shortcutTraversal.addRel(casting.getRolePlayer().getName());
            }
        });
    }

    @Override
    public Collection<MultiTraversal> getMultiTraversals(String start) {
        Collection<String> castingNames = new HashSet<>();

        Stream<MultiTraversal> traversals = castings.stream().flatMap(casting -> {

            String castingName = UUID.randomUUID().toString();
            castingNames.add(castingName);

            return multiTraversalsFromCasting(start, castingName, casting);
        });

        Stream<MultiTraversal> distinctCastingTraversals = castingNames.stream().flatMap(
                castingName -> castingNames.stream().map(otherName -> makeDistinctCastingPattern(castingName, otherName))
        );

        return Stream.concat(traversals, distinctCastingTraversals).collect(toSet());
    }

    @Override
    public Collection<VarAdmin> getInnerVars() {
        return castings.stream().flatMap(casting -> {
            Stream.Builder<VarAdmin> builder = Stream.builder();
            builder.add(casting.getRolePlayer());
            casting.getRoleType().ifPresent(builder::add);
            return builder.build();
        }).collect(toSet());
    }

    private Stream<MultiTraversal> multiTraversalsFromCasting(String start, String castingName, VarAdmin.Casting casting) {
        Optional<VarAdmin> roleType = casting.getRoleType();

        if (roleType.isPresent()) {
            return addRelatesPattern(start, castingName, roleType.get(), casting.getRolePlayer());
        } else {
            return addRelatesPattern(start, castingName, casting.getRolePlayer());
        }
    }

    /**
     * Add some patterns where this variable is a relation and the given variable is a roleplayer of that relation
     * @param rolePlayer a variable that is a roleplayer of this relation
     */
    private Stream<MultiTraversal> addRelatesPattern(String start, String casting, VarAdmin rolePlayer) {
        String other = rolePlayer.getName();

        return Stream.of(
                // Pattern between relation and casting
                new MultiTraversalImpl(
                        new FragmentImpl(t -> t.out(CASTING.getLabel()), EDGE_BOUNDED, start, casting),
                        new FragmentImpl(t -> t.in(CASTING.getLabel()), EDGE_UNBOUNDED, casting, start)
                ),
                // Pattern between casting and roleplayer
                new MultiTraversalImpl(
                        new FragmentImpl(t -> t.out(ROLE_PLAYER.getLabel()), EDGE_UNIQUE, casting, other),
                        new FragmentImpl(t -> t.in(ROLE_PLAYER.getLabel()), EDGE_BOUNDED, other, casting)
                )
        );
    }

    /**
     * Add some patterns where this variable is a relation relating the given roleplayer as the given roletype
     * @param roleType a variable that is the roletype of the given roleplayer
     * @param rolePlayer a variable that is a roleplayer of this relation
     */
    private Stream<MultiTraversal> addRelatesPattern(String start, String casting, VarAdmin roleType, VarAdmin rolePlayer) {
        String roletypeName = roleType.getName();
        String roleplayerName = rolePlayer.getName();

        return Stream.of(
                // Pattern between relation and casting
                new MultiTraversalImpl(
                        new FragmentImpl(t -> t.out(CASTING.getLabel()), EDGE_BOUNDED, start, casting),
                        new FragmentImpl(t -> t.in(CASTING.getLabel()), EDGE_UNBOUNDED, casting, start)
                ),

                // Pattern between casting and roleplayer
                new MultiTraversalImpl(
                        new FragmentImpl(t -> t.out(ROLE_PLAYER.getLabel()), EDGE_UNIQUE, casting, roleplayerName),
                        new FragmentImpl(t -> t.in(ROLE_PLAYER.getLabel()), EDGE_BOUNDED, roleplayerName, casting)
                ),

                // Pattern between casting and role type
                new MultiTraversalImpl(
                        new FragmentImpl(t -> t.out(ISA.getLabel()), EDGE_UNIQUE, casting, roletypeName),
                        new FragmentImpl(t -> t.in(ISA.getLabel()), EDGE_UNBOUNDED, roletypeName, casting)
                )
        );
    }

    /**
     * @param casting a casting variable name
     * @param otherCastingId a different casting variable name
     * @return a MultiTraversal that indicates two castings are unique
     */
    private MultiTraversal makeDistinctCastingPattern(String casting, String otherCastingId) {
        return new MultiTraversalImpl(new FragmentImpl(t -> t.where(P.neq(otherCastingId)), DISTINCT_CASTING, casting));
    }
}
