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

package grakn.core.graql.executor.property;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.Role;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.executor.WriteExecutor;
import grakn.core.graql.gremlin.EquivalentFragmentSet;
import grakn.core.graql.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.reasoner.atom.Atomic;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.query.ReasonerQuery;
import graql.lang.property.IsaProperty;
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

import static grakn.core.common.util.CommonUtil.toImmutableSet;
import static grakn.core.graql.gremlin.sets.EquivalentFragmentSets.rolePlayer;
import static grakn.core.graql.reasoner.utils.ReasonerUtils.getUserDefinedIdPredicate;

public class RelationExecutor implements PropertyExecutor.Insertable {

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
        }).collect(toImmutableSet());

        ImmutableSet<EquivalentFragmentSet> distinctCastingTraversals = castingNames.stream().flatMap(
                castingName -> castingNames.stream()
                        .filter(otherName -> !otherName.equals(castingName))
                        .map(otherName -> EquivalentFragmentSets.neq(property, castingName, otherName))
        ).collect(toImmutableSet());

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
    public Atomic atomic(ReasonerQuery parent, Statement statement, Set<Statement> otherStatements) {
        //set varName as user defined if reified
        //reified if contains more properties than the RelationProperty itself and potential IsaProperty
        boolean isReified = statement.properties().stream()
                .filter(prop -> !RelationProperty.class.isInstance(prop))
                .anyMatch(prop -> !IsaProperty.class.isInstance(prop));
        Statement relVar = isReified ? new Statement(var.asUserDefined()) : new Statement(var);

        for (RelationProperty.RolePlayer rp : property.relationPlayers()) {
            Statement rolePattern = rp.getRole().orElse(null);
            Statement rolePlayer = rp.getPlayer();
            if (rolePattern != null) {
                Variable roleVar = rolePattern.var();
                //look for indirect role definitions
                IdPredicate roleId = getUserDefinedIdPredicate(roleVar, otherStatements, parent);
                if (roleId != null) {
                    Concept concept = parent.tx().getConcept(roleId.getPredicate());
                    if (concept != null) {
                        if (concept.isRole()) {
                            Label roleLabel = concept.asSchemaConcept().label();
                            rolePattern = new Statement(roleVar).type(roleLabel.getValue());
                        } else {
                            throw GraqlQueryException.nonRoleIdAssignedToRoleVariable(statement);
                        }
                    }
                }
                relVar = relVar.rel(rolePattern, rolePlayer);
            } else relVar = relVar.rel(rolePlayer);
        }

        //isa part
        IsaProperty isaProp = statement.getProperty(IsaProperty.class).orElse(null);
        IdPredicate predicate = null;

        //if no isa property present generate type variable
        Variable typeVariable = isaProp != null ? isaProp.type().var() : new Variable();

        //Isa present
        if (isaProp != null) {
            Statement isaVar = isaProp.type();
            String label = isaVar.getType().orElse(null);
            if (label != null) {
                predicate = IdPredicate.create(typeVariable, Label.of(label), parent);
            } else {
                typeVariable = isaVar.var();
                predicate = getUserDefinedIdPredicate(typeVariable, otherStatements, parent);
            }
        }
        ConceptId predicateId = predicate != null ? predicate.getPredicate() : null;
        relVar = isaProp != null && isaProp.isExplicit() ?
                relVar.isaX(new Statement(typeVariable.asUserDefined())) :
                relVar.isa(new Statement(typeVariable.asUserDefined()));
        return RelationAtom.create(relVar, typeVariable, predicateId, parent);
    }

    @Override
    public Set<PropertyExecutor.Writer> insertExecutors() {
        return ImmutableSet.of(new InsertRelation());
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
                    .flatMap(relationPlayer -> Stream.of(relationPlayer.getPlayer(), getRole(relationPlayer)))
                    .map(statement -> statement.var())
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
                Statement roleVar = getRole(relationPlayer);

                Role role = executor.getConcept(roleVar.var()).asRole();
                Thing roleplayer = executor.getConcept(relationPlayer.getPlayer().var()).asThing();
                relation.assign(role, roleplayer);
            });
        }

        private Statement getRole(RelationProperty.RolePlayer relationPlayer) {
            return relationPlayer.getRole().orElseThrow(GraqlQueryException::insertRolePlayerWithoutRoleType);
        }
    }
}
