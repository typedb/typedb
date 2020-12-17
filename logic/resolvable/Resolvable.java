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
 */

package grakn.core.logic.resolvable;

import grakn.core.common.exception.GraknException;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IIDConstraint;
import grakn.core.pattern.constraint.thing.IsConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.constraint.type.AbstractConstraint;
import grakn.core.pattern.constraint.type.LabelConstraint;
import grakn.core.pattern.constraint.type.OwnsConstraint;
import grakn.core.pattern.constraint.type.PlaysConstraint;
import grakn.core.pattern.constraint.type.RegexConstraint;
import grakn.core.pattern.constraint.type.RelatesConstraint;
import grakn.core.pattern.constraint.type.SubConstraint;
import grakn.core.pattern.constraint.type.TypeConstraint;
import grakn.core.pattern.constraint.type.ValueTypeConstraint;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class Resolvable {

    public abstract Conjunction conjunction();

    public boolean isRetrievable() {
        return false;
    }

    public boolean isConcludable() {
        return false;
    }

    public Retrievable asRetrievable() {
        throw GraknException.of(INVALID_CASTING);
    }

    public Concludable<?> asConcludable() {
        throw GraknException.of(INVALID_CASTING);
    }

    public static Set<Retrievable> extractRetrievables(Conjunction conjunction, Set<Concludable<?>> concludables) {
        return Extractor.from(conjunction, concludables).split();
    }

    public static class Extractor {
        private final Conjunction conjunction;
        private final Set<Concludable<?>> concludables;

        private final Set<Constraint> concludableConstraints = set();
        private final Map<Variable, Variable> variablesMap = map(); // Map from variables in original conjunction to new subConjunction
        private final Set<Constraint> visitedConstraints = set();
        private final Set<Set<Variable>> connectedSubgraphs = set();
        private Set<Variable> connectedSubgraph;


        public Extractor(Conjunction conjunction, Set<Concludable<?>> concludables) {
            this.conjunction = conjunction;
            this.concludables = concludables;
        }

        public static Extractor from(Conjunction conjunction, Set<Concludable<?>> concludables) {
            return new Extractor(conjunction, concludables);
        }

        public Set<Retrievable> split() {
            concludables.forEach(concludable -> concludableConstraints.add(concludable.constraint()));
            visitedConstraints.addAll(concludableConstraints);

            conjunction.variables().forEach(var -> {
                if (variablesMap.get(var) == null) {
                    connectedSubgraph = set();
                    if (var.isThing()) addVariable(var.asThing());
                    else if (var.isThing()) addVariable(var.asType());
                    else throw GraknException.of(ILLEGAL_STATE);
                    connectedSubgraphs.add(connectedSubgraph);
                }
            });
            return connectedSubgraphs.stream().map(subGraph -> new Retrievable(new Conjunction(subGraph, set()))).collect(Collectors.toSet());
        }

        private void addToConnected(Variable copy) {
            connectedSubgraph.add(copy);
        }

        private TypeVariable addVariable(TypeVariable variable) {
            TypeVariable copy = variablesMap.get(variable).asType();
            if (copy == null) {
                copy = TypeVariable.of(variable.identifier());
                addToConnected(copy);
                variablesMap.put(variable, copy);
                variable.constraints().forEach(this::addConstraint);
                variable.constrainedBy().forEach(this::addConstraint);
            }
            return copy;
        }

        private ThingVariable addVariable(ThingVariable variable) {
            ThingVariable copy = variablesMap.get(variable).asThing();
            if (copy == null) {
                copy = ThingVariable.of(variable.identifier());
                addToConnected(copy);
                variablesMap.put(variable, copy);
                variable.constraints().forEach(this::addConstraint);
                variable.constrainedBy().forEach(this::addConstraint);
            }
            return copy;
        }

        private void addConstraint(Constraint constraint) {
            if (constraint.isThing()) addConstraint(constraint.asThing());
            else if (constraint.isType()) addConstraint(constraint.asType());
            else throw GraknException.of(ILLEGAL_STATE);
        }

        private void addConstraint(ThingConstraint thingConstraint) {
            if (!visitedConstraints.contains(thingConstraint)) {
                visitedConstraints.add(thingConstraint);
                if (thingConstraint.isRelation()) copyConstraint(thingConstraint.asRelation());
                else if (thingConstraint.isHas()) copyConstraint(thingConstraint.asHas());
                else if (thingConstraint.isIs()) copyConstraint(thingConstraint.asIs());
                else if (thingConstraint.isValue()) copyConstraint(thingConstraint.asValue());
                else if (thingConstraint.isIsa()) copyConstraint(thingConstraint.asIsa());
                else if (thingConstraint.isIID()) copyConstraint(thingConstraint.asIID());
                else throw GraknException.of(ILLEGAL_STATE);
            }
        }

        private void addConstraint(TypeConstraint typeConstraint) {
            if (!visitedConstraints.contains(typeConstraint)) {
                visitedConstraints.add(typeConstraint);
                // TODO Some constraints such as abstract are not needed as this is only queryable in a schema session?
                if (typeConstraint.isAbstract()) copyConstraint(typeConstraint.asAbstract());
                else if (typeConstraint.isIs()) copyConstraint(typeConstraint.asIs());
                else if (typeConstraint.isLabel()) copyConstraint(typeConstraint.asLabel());
                else if (typeConstraint.isOwns()) copyConstraint(typeConstraint.asOwns());
                else if (typeConstraint.isPlays()) copyConstraint(typeConstraint.asPlays());
                else if (typeConstraint.isRelates()) copyConstraint(typeConstraint.asRelates());
                else if (typeConstraint.isRegex()) copyConstraint(typeConstraint.asRegex());
                else if (typeConstraint.isSub()) copyConstraint(typeConstraint.asSub());
                else if (typeConstraint.isValueType()) copyConstraint(typeConstraint.asValueType());
                else throw GraknException.of(ILLEGAL_STATE);
            }
        }

        private void copyConstraint(RelationConstraint constraint) {
            addVariable(constraint.owner()).asThing().relation(copyRolePlayers(constraint.players()));
        }

        private List<RelationConstraint.RolePlayer> copyRolePlayers(List<RelationConstraint.RolePlayer> players) {
            return players.stream().map(rolePlayer -> {
                TypeVariable roleTypeCopy = rolePlayer.roleType().isPresent() ? addVariable(rolePlayer.roleType().get()) : null;
                ThingVariable playerCopy = addVariable(rolePlayer.player());
                RelationConstraint.RolePlayer rolePlayerCopy = new RelationConstraint.RolePlayer(roleTypeCopy, playerCopy);
                rolePlayerCopy.addRoleTypeHints(rolePlayer.roleTypeHints());
                return rolePlayerCopy;
            }).collect(Collectors.toList());
        }

        private void copyConstraint(HasConstraint constraint) {
            addVariable(constraint.owner()).has(addVariable(constraint.attribute()));
        }

        private void copyConstraint(IsConstraint constraint) {
            addVariable(constraint.owner()).is(addVariable(constraint.variable()));
        }

        private void copyConstraint(ValueConstraint<?> constraint) {
            ThingVariable var = addVariable(constraint.owner());
            if (constraint.isLong()) var.valueLong(constraint.asLong().predicate().asEquality(), constraint.asLong().value());
            else if (constraint.isDouble()) var.valueDouble(constraint.asDouble().predicate().asEquality(), constraint.asDouble().value());
            else if (constraint.isBoolean()) var.valueBoolean(constraint.asBoolean().predicate().asEquality(), constraint.asBoolean().value());
            else if (constraint.isString()) var.valueString(constraint.asString().predicate(), constraint.asString().value());
            else if (constraint.isDateTime()) var.valueDateTime(constraint.asDateTime().predicate().asEquality(), constraint.asDateTime().value());
            else if (constraint.isVariable()) {
                ThingVariable attributeVarCopy = addVariable(constraint.asVariable().value());
                var.valueVariable(constraint.asValue().predicate().asEquality(), attributeVarCopy);
            } else throw GraknException.of(ILLEGAL_STATE);
        }

        private void copyConstraint(IsaConstraint constraint) {
            addVariable(constraint.owner()).isa(addVariable(constraint.type()), constraint.isExplicit());
        }

        private void copyConstraint(IIDConstraint constraint) {
            addVariable(constraint.owner()).iid(constraint.iid());
        }

        private void copyConstraint(AbstractConstraint abstractConstraint) {
            addVariable(abstractConstraint.owner()).makeAbstract();
        }

        private void copyConstraint(grakn.core.pattern.constraint.type.IsConstraint constraint) {
            addVariable(constraint.owner()).is(addVariable(constraint.variable()));
        }

        private void copyConstraint(LabelConstraint constraint) {
            addVariable(constraint.owner()).label(constraint.properLabel());
        }

        private void copyConstraint(OwnsConstraint constraint) {
            addVariable(constraint.owner()).owns(addVariable(constraint.attribute()),
                                                 constraint.overridden().map(this::addVariable).orElse(null),
                                                 constraint.isKey());
        }

        private void copyConstraint(PlaysConstraint constraint) {
            addVariable(constraint.owner()).plays(constraint.relation().map(this::addVariable).orElse(null),
                                                  addVariable(constraint.role()),
                                                  constraint.overridden().map(this::addVariable).orElse(null));
        }

        private void copyConstraint(RelatesConstraint constraint) {
            addVariable(constraint.owner()).relates(addVariable(constraint.role()),
                                                    constraint.overridden().map(this::addVariable).orElse(null));
        }

        private void copyConstraint(RegexConstraint constraint) {
            addVariable(constraint.owner()).regex(constraint.regex());
        }

        private void copyConstraint(SubConstraint constraint) {
            addVariable(constraint.owner()).sub(addVariable(constraint.type()), constraint.isExplicit());
        }

        private void copyConstraint(ValueTypeConstraint constraint) {
            addVariable(constraint.owner()).valueType(constraint.valueType());
        }
    }
}
