/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.ShortcutTraversal;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.util.CommonUtil;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.gremlin.fragment.Fragments.distinctCasting;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inCasting;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inIsaCastings;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inRolePlayer;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outCasting;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outIsaCastings;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outRolePlayer;
import static ai.grakn.graql.internal.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

public class RelationProperty extends AbstractVarProperty implements UniqueVarProperty, VarPropertyInternal {

    private final ImmutableMultiset<RelationPlayer> relationPlayers;

    public RelationProperty(ImmutableMultiset<RelationPlayer> relationPlayers) {
        this.relationPlayers = relationPlayers;
    }

    public Stream<RelationPlayer> getRelationPlayers() {
        return relationPlayers.stream();
    }

    @Override
    public void buildString(StringBuilder builder) {
        builder.append("(").append(relationPlayers.stream().map(Object::toString).collect(joining(", "))).append(")");
    }

    @Override
    public void modifyShortcutTraversal(ShortcutTraversal shortcutTraversal) {
        relationPlayers.forEach(relationPlayer -> {
            Optional<VarAdmin> roleType = relationPlayer.getRoleType();

            if (roleType.isPresent()) {
                Optional<String> roleTypeName = roleType.get().getTypeName();

                if (roleTypeName.isPresent()) {
                    shortcutTraversal.addRel(roleTypeName.get(), relationPlayer.getRolePlayer().getVarName());
                } else {
                    shortcutTraversal.setInvalid();
                }

            } else {
                shortcutTraversal.addRel(relationPlayer.getRolePlayer().getVarName());
            }
        });
    }

    @Override
    public Collection<EquivalentFragmentSet> match(String start) {
        Collection<String> castingNames = new HashSet<>();

        ImmutableSet<EquivalentFragmentSet> traversals = relationPlayers.stream().flatMap(relationPlayer -> {

            String castingName = UUID.randomUUID().toString();
            castingNames.add(castingName);

            return equivalentFragmentSetFromCasting(start, castingName, relationPlayer);
        }).collect(toImmutableSet());

        ImmutableSet<EquivalentFragmentSet> distinctCastingTraversals = castingNames.stream().flatMap(
                castingName -> castingNames.stream()
                        .filter(otherName -> !otherName.equals(castingName))
                        .map(otherName -> makeDistinctCastingPattern(castingName, otherName)
                )
        ).collect(toImmutableSet());

        return Sets.union(traversals, distinctCastingTraversals);
    }

    @Override
    public Stream<VarAdmin> getTypes() {
        return relationPlayers.stream().map(RelationPlayer::getRoleType).flatMap(CommonUtil::optionalToStream);
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return relationPlayers.stream().flatMap(relationPlayer -> {
            Stream.Builder<VarAdmin> builder = Stream.builder();
            builder.add(relationPlayer.getRolePlayer());
            relationPlayer.getRoleType().ifPresent(builder::add);
            return builder.build();
        });
    }

    private Stream<EquivalentFragmentSet> equivalentFragmentSetFromCasting(String start, String castingName, RelationPlayer relationPlayer) {
        Optional<VarAdmin> roleType = relationPlayer.getRoleType();

        if (roleType.isPresent()) {
            return addRelatesPattern(start, castingName, roleType.get(), relationPlayer.getRolePlayer());
        } else {
            return addRelatesPattern(start, castingName, relationPlayer.getRolePlayer());
        }
    }

    /**
     * Add some patterns where this variable is a relation and the given variable is a roleplayer of that relation
     * @param rolePlayer a variable that is a roleplayer of this relation
     */
    private Stream<EquivalentFragmentSet> addRelatesPattern(String start, String casting, VarAdmin rolePlayer) {
        String other = rolePlayer.getVarName();

        return Stream.of(
                // Pattern between relation and casting
                EquivalentFragmentSet.create(outCasting(start, casting), inCasting(casting, start)),
                // Pattern between casting and roleplayer
                EquivalentFragmentSet.create(outRolePlayer(casting, other), inRolePlayer(other, casting))
        );
    }

    /**
     * Add some patterns where this variable is a relation relating the given roleplayer as the given roletype
     * @param roleType a variable that is the roletype of the given roleplayer
     * @param rolePlayer a variable that is a roleplayer of this relation
     */
    private Stream<EquivalentFragmentSet> addRelatesPattern(String start, String casting, VarAdmin roleType, VarAdmin rolePlayer) {
        String roletypeName = roleType.getVarName();
        String roleplayerName = rolePlayer.getVarName();

        return Stream.of(
                // Pattern between relation and casting
                EquivalentFragmentSet.create(outCasting(start, casting), inCasting(casting, start)),

                // Pattern between casting and roleplayer
                EquivalentFragmentSet.create(
                        outRolePlayer(casting, roleplayerName),
                        inRolePlayer(roleplayerName, casting)
                ),

                // Pattern between casting and role type
                EquivalentFragmentSet.create(
                        outIsaCastings(casting, roletypeName), inIsaCastings(roletypeName, casting)
                )
        );
    }

    /**
     * @param casting a casting variable name
     * @param otherCastingId a different casting variable name
     * @return a EquivalentFragmentSet that indicates two castings are unique
     */
    private EquivalentFragmentSet makeDistinctCastingPattern(String casting, String otherCastingId) {
        return EquivalentFragmentSet.create(
                distinctCasting(casting, otherCastingId),
                distinctCasting(otherCastingId, casting)
        );
    }

    @Override
    public void checkValidProperty(GraknGraph graph, VarAdmin var) throws IllegalStateException {

        Set<String> roleTypes = relationPlayers.stream()
                .map(RelationPlayer::getRoleType).flatMap(CommonUtil::optionalToStream)
                .map(VarAdmin::getTypeName).flatMap(CommonUtil::optionalToStream)
                .collect(toSet());

        Optional<String> maybeName =
                var.getProperty(IsaProperty.class).map(IsaProperty::getType).flatMap(VarAdmin::getTypeName);

        maybeName.ifPresent(name -> {
            RelationType relationType = graph.getRelationType(name);

            if (relationType == null) {
                throw new IllegalStateException(ErrorMessage.NOT_A_RELATION_TYPE.getMessage(name));
            }

            Collection<RelationType> relationTypes = relationType.subTypes();

            Set<String> validRoles = relationTypes.stream()
                    .flatMap(r -> r.hasRoles().stream())
                    .map(Type::getName)
                    .collect(toSet());

            String errors = roleTypes.stream().filter(roleType -> !validRoles.contains(roleType)).map(roleType ->
                    ErrorMessage.NOT_ROLE_IN_RELATION.getMessage(roleType, name, validRoles)
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
        if (!var.hasProperty(IsaProperty.class)) {
            throw new IllegalStateException(ErrorMessage.INSERT_RELATION_WITHOUT_ISA.getMessage());
        }
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
        Relation relation = concept.asRelation();
        relationPlayers.forEach(relationPlayer -> addRoleplayer(insertQueryExecutor, relation, relationPlayer));
    }

    /**
     * Add a roleplayer to the given relation
     * @param relation the concept representing the relation
     * @param relationPlayer a casting between a role type and role player
     */
    private void addRoleplayer(InsertQueryExecutor insertQueryExecutor, Relation relation, RelationPlayer relationPlayer) {
        VarAdmin roleVar = relationPlayer.getRoleType().orElseThrow(
                () -> new IllegalStateException(ErrorMessage.INSERT_RELATION_WITHOUT_ROLE_TYPE.getMessage())
        );

        RoleType roleType = insertQueryExecutor.getConcept(roleVar).asRoleType();
        Instance roleplayer = insertQueryExecutor.getConcept(relationPlayer.getRolePlayer()).asInstance();
        relation.putRolePlayer(roleType, roleplayer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RelationProperty that = (RelationProperty) o;

        return relationPlayers.equals(that.relationPlayers);

    }

    @Override
    public int hashCode() {
        return relationPlayers.hashCode();
    }
}
