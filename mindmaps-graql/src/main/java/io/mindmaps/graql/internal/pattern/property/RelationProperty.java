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

import com.google.common.collect.ImmutableSet;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.*;
import io.mindmaps.graql.admin.UniqueVarProperty;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.Fragment;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.ShortcutTraversal;
import io.mindmaps.graql.internal.query.InsertQueryExecutor;
import io.mindmaps.graql.internal.util.CommonUtil;
import io.mindmaps.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.*;
import java.util.stream.Stream;

import static io.mindmaps.graql.internal.gremlin.FragmentPriority.DISTINCT_CASTING;
import static io.mindmaps.graql.internal.gremlin.FragmentPriority.EDGE_BOUNDED;
import static io.mindmaps.graql.internal.gremlin.FragmentPriority.EDGE_UNBOUNDED;
import static io.mindmaps.graql.internal.gremlin.FragmentPriority.EDGE_UNIQUE;
import static io.mindmaps.util.Schema.EdgeLabel.CASTING;
import static io.mindmaps.util.Schema.EdgeLabel.ISA;
import static io.mindmaps.util.Schema.EdgeLabel.ROLE_PLAYER;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

public class RelationProperty extends AbstractVarProperty implements UniqueVarProperty, VarPropertyInternal {

    private final Set<VarAdmin.Casting> castings;

    public RelationProperty(Set<VarAdmin.Casting> castings) {
        this.castings = ImmutableSet.copyOf(castings);
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
    public Collection<MultiTraversal> match(String start) {
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
    public Stream<VarAdmin> getTypes() {
        return castings.stream().map(VarAdmin.Casting::getRoleType).flatMap(CommonUtil::optionalToStream);
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return castings.stream().flatMap(casting -> {
            Stream.Builder<VarAdmin> builder = Stream.builder();
            builder.add(casting.getRolePlayer());
            casting.getRoleType().ifPresent(builder::add);
            return builder.build();
        });
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
                MultiTraversal.create(
                        Fragment.create(t -> t.out(CASTING.getLabel()), EDGE_BOUNDED, start, casting),
                        Fragment.create(t -> t.in(CASTING.getLabel()), EDGE_UNBOUNDED, casting, start)
                ),
                // Pattern between casting and roleplayer
                MultiTraversal.create(
                        Fragment.create(t -> t.out(ROLE_PLAYER.getLabel()), EDGE_UNIQUE, casting, other),
                        Fragment.create(t -> t.in(ROLE_PLAYER.getLabel()), EDGE_BOUNDED, other, casting)
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
                MultiTraversal.create(
                        Fragment.create(t -> t.out(CASTING.getLabel()), EDGE_BOUNDED, start, casting),
                        Fragment.create(t -> t.in(CASTING.getLabel()), EDGE_UNBOUNDED, casting, start)
                ),

                // Pattern between casting and roleplayer
                MultiTraversal.create(
                        Fragment.create(t -> t.out(ROLE_PLAYER.getLabel()), EDGE_UNIQUE, casting, roleplayerName),
                        Fragment.create(t -> t.in(ROLE_PLAYER.getLabel()), EDGE_BOUNDED, roleplayerName, casting)
                ),

                // Pattern between casting and role type
                MultiTraversal.create(
                        Fragment.create(t -> t.out(ISA.getLabel()), EDGE_UNIQUE, casting, roletypeName),
                        Fragment.create(t -> t.in(ISA.getLabel()), EDGE_UNBOUNDED, roletypeName, casting)
                )
        );
    }

    /**
     * @param casting a casting variable name
     * @param otherCastingId a different casting variable name
     * @return a MultiTraversal that indicates two castings are unique
     */
    private MultiTraversal makeDistinctCastingPattern(String casting, String otherCastingId) {
        return MultiTraversal.create(Fragment.create(t -> t.where(P.neq(otherCastingId)), DISTINCT_CASTING, casting));
    }

    @Override
    public void checkValidProperty(MindmapsGraph graph, VarAdmin var) throws IllegalStateException {

        Set<String> roleTypes = castings.stream()
                .map(VarAdmin.Casting::getRoleType).flatMap(CommonUtil::optionalToStream)
                .map(VarAdmin::getIdOnly).flatMap(CommonUtil::optionalToStream)
                .collect(toSet());

        Optional<String> maybeId = var.getType().flatMap(VarAdmin::getIdOnly);

        maybeId.ifPresent(typeId -> {
            RelationType relationType = graph.getRelationType(typeId);

            if (relationType == null) {
                throw new IllegalStateException(ErrorMessage.NOT_A_RELATION_TYPE.getMessage(typeId));
            }

            Collection<RelationType> relationTypes = relationType.subTypes();

            Set<String> validRoles = relationTypes.stream()
                    .flatMap(r -> r.hasRoles().stream())
                    .map(Concept::getId)
                    .collect(toSet());

            String errors = roleTypes.stream().filter(roleType -> !validRoles.contains(roleType)).map(roleType ->
                    ErrorMessage.NOT_ROLE_IN_RELATION.getMessage(roleType, typeId, validRoles)
            ).collect(joining("\n"));

            if (!errors.equals("")) {
                throw new IllegalStateException(errors);
            }
        });

        // Check all role types exist
        roleTypes.forEach(roleId -> {
            if (graph.getRoleType(roleId) == null) {
                throw new IllegalStateException(ErrorMessage.NOT_A_ROLE_TYPE.getMessage(roleId, roleId));
            }
        });
    }

    @Override
    public void checkInsertable(VarAdmin var) throws IllegalStateException {
        if (!var.getType().isPresent()) {
            throw new IllegalStateException(ErrorMessage.INSERT_RELATION_WITHOUT_ISA.getMessage());
        }
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
        Relation relation = concept.asRelation();
        castings.forEach(casting -> addCasting(insertQueryExecutor, relation, casting));
    }

    /**
     * Add a roleplayer to the given relation
     * @param relation the concept representing the relation
     * @param casting a casting between a role type and role player
     */
    private void addCasting(InsertQueryExecutor insertQueryExecutor, Relation relation, VarAdmin.Casting casting) {
        VarAdmin roleVar = casting.getRoleType().orElseThrow(
                () -> new IllegalStateException(ErrorMessage.INSERT_RELATION_WITHOUT_ROLE_TYPE.getMessage())
        );

        RoleType roleType = insertQueryExecutor.getConcept(roleVar).asRoleType();
        Instance roleplayer = insertQueryExecutor.getConcept(casting.getRolePlayer()).asInstance();
        relation.putRolePlayer(roleType, roleplayer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RelationProperty that = (RelationProperty) o;

        return castings.equals(that.castings);

    }

    @Override
    public int hashCode() {
        return castings.hashCode();
    }
}
