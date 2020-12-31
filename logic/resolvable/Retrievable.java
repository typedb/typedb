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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class Retrievable extends Resolvable {

    private final Conjunction conjunction;

    public Retrievable(Conjunction conjunction) {
        this.conjunction = conjunction;
    }

    public static Set<Retrievable> extractFrom(Conjunction conjunction, Set<Concludable<?>> toExclude) {
        return Retrievable.Extractor.from(conjunction, toExclude).extract();
    }

    @Override
    public Conjunction conjunction() {
        return conjunction;
    }

    @Override
    public Retrievable asRetrievable() {
        return this;
    }

    @Override
    public boolean isRetrievable() {
        return true;
    }

    public static class Extractor {
        private final Conjunction conjunction;
        private final Set<Concludable<?>> concludables;

        private final Map<Variable, Variable> variablesMap = new HashMap<>(); // Map from variables in original conjunction to new subConjunction
        private final Set<Constraint> visitedConstraints = new HashSet<>();
        private final Set<Set<Variable>> connectedSubgraphs = new HashSet<>();
        private Set<Variable> connectedSubgraph;


        public Extractor(Conjunction conjunction, Set<Concludable<?>> concludables) {
            this.conjunction = conjunction;
            this.concludables = concludables;
        }

        public static Extractor from(Conjunction conjunction, Set<Concludable<?>> concludables) {
            return new Extractor(conjunction, concludables);
        }

        public Set<Retrievable> extract() {
            concludables.forEach(concludable -> visitedConstraints.addAll(concludable.coreConstraints()));
            conjunction.variables().stream().filter(var -> var.identifier().reference().isName()).forEach(var -> {
                if (variablesMap.get(var) == null) {
                    connectedSubgraph = new HashSet<>();
                    if (var.isThing()) registerThingVariable(var.asThing());
                    else if (var.isType()) registerTypeVariable(var.asType());
                    else throw GraknException.of(ILLEGAL_STATE);
                    connectedSubgraphs.add(connectedSubgraph);
                }
            });
            return connectedSubgraphs.stream()
                    .filter(subGraph -> !(subGraph.size() == 1 && subGraph.iterator().next().constraints().size() == 0))
                    .map(subGraph -> new Retrievable(new Conjunction(subGraph, set()))).collect(Collectors.toSet());
        }

        private void addToConnected(Variable copy) {
            connectedSubgraph.add(copy);
        }

        private TypeVariable registerTypeVariable(TypeVariable variable) {
            return registerVariable(variable).asType();
        }

        private ThingVariable registerThingVariable(ThingVariable variable) {
            return registerVariable(variable).asThing();
        }

        private Variable registerVariable(Variable variable) {
            Variable copy = variablesMap.get(variable);
            if (copy == null) {
                copy = copyVariable(variable);
                addToConnected(copy);
                variablesMap.put(variable, copy);
                variable.constraints().forEach(this::addConstraint);
                variable.constraining().forEach(this::addConstraint);
            }
            return copy;
        }

        private Variable copyVariable(Variable toCopy) {
            Variable copy;
            if (toCopy.isThing()) copy = ThingVariable.of(toCopy.identifier());
            else if (toCopy.isType()) copy = TypeVariable.of(toCopy.identifier());
            else throw GraknException.of(ILLEGAL_STATE);
            copy.addResolvedTypes(toCopy.resolvedTypes());
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
            registerThingVariable(constraint.owner()).asThing().relation(copyRolePlayers(constraint.players()));
        }

        private List<RelationConstraint.RolePlayer> copyRolePlayers(List<RelationConstraint.RolePlayer> players) {
            return players.stream().map(rolePlayer -> {
                TypeVariable roleTypeCopy = rolePlayer.roleType().isPresent() ? registerTypeVariable(rolePlayer.roleType().get()) : null;
                ThingVariable playerCopy = registerThingVariable(rolePlayer.player());
                return new RelationConstraint.RolePlayer(roleTypeCopy, playerCopy);
            }).collect(Collectors.toList());
        }

        private void copyConstraint(HasConstraint constraint) {
            registerThingVariable(constraint.owner()).has(registerThingVariable(constraint.attribute()));
        }

        private void copyConstraint(IsConstraint constraint) {
            registerThingVariable(constraint.owner()).is(registerThingVariable(constraint.variable()));
        }

        private void copyConstraint(ValueConstraint<?> constraint) {
            ThingVariable var = registerThingVariable(constraint.owner());
            if (constraint.isLong())
                var.valueLong(constraint.asLong().predicate().asEquality(), constraint.asLong().value());
            else if (constraint.isDouble())
                var.valueDouble(constraint.asDouble().predicate().asEquality(), constraint.asDouble().value());
            else if (constraint.isBoolean())
                var.valueBoolean(constraint.asBoolean().predicate().asEquality(), constraint.asBoolean().value());
            else if (constraint.isString())
                var.valueString(constraint.asString().predicate(), constraint.asString().value());
            else if (constraint.isDateTime())
                var.valueDateTime(constraint.asDateTime().predicate().asEquality(), constraint.asDateTime().value());
            else if (constraint.isVariable()) {
                ThingVariable attributeVarCopy = registerThingVariable(constraint.asVariable().value());
                var.valueVariable(constraint.asValue().predicate().asEquality(), attributeVarCopy);
            } else throw GraknException.of(ILLEGAL_STATE);
        }

        private void copyConstraint(IsaConstraint constraint) {
            registerThingVariable(constraint.owner()).isa(registerTypeVariable(constraint.type()), constraint.isExplicit());
        }

        private void copyConstraint(IIDConstraint constraint) {
            registerThingVariable(constraint.owner()).iid(constraint.iid());
        }

        private void copyConstraint(AbstractConstraint abstractConstraint) {
            registerTypeVariable(abstractConstraint.owner()).setAbstract();
        }

        private void copyConstraint(grakn.core.pattern.constraint.type.IsConstraint constraint) {
            registerTypeVariable(constraint.owner()).is(registerTypeVariable(constraint.variable()));
        }

        private void copyConstraint(LabelConstraint constraint) {
            registerTypeVariable(constraint.owner()).label(constraint.properLabel());
        }

        private void copyConstraint(OwnsConstraint constraint) {
            registerTypeVariable(constraint.owner()).owns(registerTypeVariable(constraint.attribute()),
                                                          constraint.overridden().map(this::registerTypeVariable).orElse(null),
                                                          constraint.isKey());
        }

        private void copyConstraint(PlaysConstraint constraint) {
            registerTypeVariable(constraint.owner()).plays(constraint.relation().map(this::registerTypeVariable).orElse(null),
                                                           registerTypeVariable(constraint.role()),
                                                           constraint.overridden().map(this::registerTypeVariable).orElse(null));
        }

        private void copyConstraint(RelatesConstraint constraint) {
            registerTypeVariable(constraint.owner()).relates(registerTypeVariable(constraint.role()),
                                                             constraint.overridden().map(this::registerTypeVariable).orElse(null));
        }

        private void copyConstraint(RegexConstraint constraint) {
            registerTypeVariable(constraint.owner()).regex(constraint.regex());
        }

        private void copyConstraint(SubConstraint constraint) {
            registerTypeVariable(constraint.owner()).sub(registerTypeVariable(constraint.type()), constraint.isExplicit());
        }

        private void copyConstraint(ValueTypeConstraint constraint) {
            registerTypeVariable(constraint.owner()).valueType(constraint.valueType());
        }
    }
}
