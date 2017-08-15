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
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.binary.RelationAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.util.CommonUtil;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.shortcut;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getUserDefinedIdPredicate;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Represents the relation property (e.g. {@code ($x, $y)} or {@code (wife: $x, husband: $y)}) on a {@link Relationship}.
 *
 * This property can be queried and inserted.
 *
 * This propert is comprised of instances of {@link RelationPlayer}, which represents associations between a
 * role-player {@link Thing} and an optional {@link Role}.
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class RelationProperty extends AbstractVarProperty implements UniqueVarProperty {

    public static RelationProperty of(ImmutableMultiset<RelationPlayer> relationPlayers) {
        return new AutoValue_RelationProperty(relationPlayers);
    }

    public abstract ImmutableMultiset<RelationPlayer> relationPlayers();

    @Override
    public void buildString(StringBuilder builder) {
        builder.append("(").append(relationPlayers().stream().map(Object::toString).collect(joining(", "))).append(")");
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        Collection<Var> castingNames = new HashSet<>();

        ImmutableSet<EquivalentFragmentSet> traversals = relationPlayers().stream().flatMap(relationPlayer -> {

            Var castingName = Graql.var();
            castingNames.add(castingName);

            return equivalentFragmentSetFromCasting(start, castingName, relationPlayer);
        }).collect(toImmutableSet());

        ImmutableSet<EquivalentFragmentSet> distinctCastingTraversals = castingNames.stream().flatMap(
                castingName -> castingNames.stream()
                        .filter(otherName -> !otherName.equals(castingName))
                        .map(otherName -> EquivalentFragmentSets.neq(this, castingName, otherName))
        ).collect(toImmutableSet());

        return Sets.union(traversals, distinctCastingTraversals);
    }

    @Override
    public Stream<VarPatternAdmin> getTypes() {
        return relationPlayers().stream().map(RelationPlayer::getRole).flatMap(CommonUtil::optionalToStream);
    }

    @Override
    public Stream<VarPatternAdmin> getInnerVars() {
        return relationPlayers().stream().flatMap(relationPlayer -> {
            Stream.Builder<VarPatternAdmin> builder = Stream.builder();
            builder.add(relationPlayer.getRolePlayer());
            relationPlayer.getRole().ifPresent(builder::add);
            return builder.build();
        });
    }

    private Stream<EquivalentFragmentSet> equivalentFragmentSetFromCasting(Var start, Var castingName, RelationPlayer relationPlayer) {
        Optional<VarPatternAdmin> roleType = relationPlayer.getRole();

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
    private Stream<EquivalentFragmentSet> addRelatesPattern(Var start, Var casting, VarPatternAdmin rolePlayer) {
        return Stream.of(shortcut(this, start, casting, rolePlayer.getVarName(), Optional.empty()));
    }

    /**
     * Add some patterns where this variable is a relation relating the given roleplayer as the given roletype
     * @param roleType a variable that is the roletype of the given roleplayer
     * @param rolePlayer a variable that is a roleplayer of this relation
     */
    private Stream<EquivalentFragmentSet> addRelatesPattern(Var start, Var casting, VarPatternAdmin roleType, VarPatternAdmin rolePlayer) {
        return Stream.of(shortcut(this, start, casting, rolePlayer.getVarName(), Optional.of(roleType.getVarName())));
    }

    @Override
    public void checkValidProperty(GraknGraph graph, VarPatternAdmin var) throws GraqlQueryException {

        Set<Label> roleTypes = relationPlayers().stream()
                .map(RelationPlayer::getRole).flatMap(CommonUtil::optionalToStream)
                .map(VarPatternAdmin::getTypeLabel).flatMap(CommonUtil::optionalToStream)
                .collect(toSet());

        Optional<Label> maybeLabel =
                var.getProperty(IsaProperty.class).map(IsaProperty::type).flatMap(VarPatternAdmin::getTypeLabel);

        maybeLabel.ifPresent(label -> {
            OntologyConcept ontologyConcept = graph.getOntologyConcept(label);

            if (ontologyConcept == null || !ontologyConcept.isRelationType()) {
                throw GraqlQueryException.notARelationType(label);
            }
        });

        // Check all role types exist
        roleTypes.forEach(roleId -> {
            SchemaConcept schemaConcept = graph.getOntologyConcept(roleId);
            if (schemaConcept == null || !schemaConcept.isRole()) {
                throw GraqlQueryException.notARoleType(roleId);
            }
        });
    }

    @Override
    public void checkInsertable(VarPatternAdmin var) throws GraqlQueryException {
        if (!var.hasProperty(IsaProperty.class)) {
            throw GraqlQueryException.insertRelationWithoutType();
        }
    }

    @Override
    public void insert(Var var, InsertQueryExecutor executor) throws GraqlQueryException {
        Relationship relationship = executor.get(var).asRelation();
        relationPlayers().forEach(relationPlayer -> addRoleplayer(executor, relationship, relationPlayer));
    }

    /**
     * Add a roleplayer to the given {@link Relationship}
     * @param relationship the concept representing the {@link Relationship}
     * @param relationPlayer a casting between a role type and role player
     */
    private void addRoleplayer(InsertQueryExecutor executor, Relationship relationship, RelationPlayer relationPlayer) {
        VarPatternAdmin roleVar = getRole(relationPlayer);

        Role role = executor.get(roleVar.getVarName()).asRole();
        Thing roleplayer = executor.get(relationPlayer.getRolePlayer().getVarName()).asThing();
        relationship.addRolePlayer(role, roleplayer);
    }

    @Override
    public Set<Var> requiredVars(Var var) {
        Stream<Var> relationPlayers = this.relationPlayers().stream()
                .flatMap(relationPlayer -> Stream.of(relationPlayer.getRolePlayer(), getRole(relationPlayer)))
                .map(VarPatternAdmin::getVarName);

        return Stream.concat(relationPlayers, Stream.of(var)).collect(toImmutableSet());
    }

    private VarPatternAdmin getRole(RelationPlayer relationPlayer) {
        return relationPlayer.getRole().orElseThrow(GraqlQueryException::insertRolePlayerWithoutRoleType);
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        //keep varName if reified, reified if contains more properties than the RelationProperty itself and potential IsaProperty
        boolean isReified = var.getProperties()
                .filter(prop -> !RelationProperty.class.isInstance(prop))
                .filter(prop -> !IsaProperty.class.isInstance(prop))
                .count() > 0;
        VarPattern relVar = (var.getVarName().isUserDefinedName() || isReified)? var.getVarName().asUserDefined() : Graql.var();

        for (RelationPlayer rp : relationPlayers()) {
            VarPatternAdmin role = rp.getRole().orElse(null);
            VarPatternAdmin rolePlayer = rp.getRolePlayer();
            if (role != null) relVar = relVar.rel(role, rolePlayer);
            else relVar = relVar.rel(rolePlayer);
        }

        //id part
        IsaProperty isaProp = var.getProperty(IsaProperty.class).orElse(null);
        IdPredicate predicate = null;
        Var typeVariable = isaProp != null? isaProp.type().getVarName().asUserDefined() : Graql.var().asUserDefined();
        //Isa present
        if (isaProp != null) {
            VarPatternAdmin isaVar = isaProp.type();
            Label label = isaVar.getTypeLabel().orElse(null);
            if (label != null) {
                VarPatternAdmin idVar = typeVariable.id(parent.graph().getOntologyConcept(label).getId()).admin();
                predicate = new IdPredicate(idVar, parent);
            } else {
                typeVariable = isaVar.getVarName();
                predicate = getUserDefinedIdPredicate(typeVariable, vars, parent);
            }
        }
        relVar = relVar.isa(typeVariable);
        return new RelationAtom(relVar.admin(), typeVariable, predicate, parent);
    }
}
