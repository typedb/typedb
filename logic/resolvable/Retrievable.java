/*
 * Copyright (C) 2021 Grakn Labs
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
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.type.TypeConstraint;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.traversal.common.Identifier;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public class Retrievable extends Resolvable<Conjunction> {

    private final Set<Identifier.Variable.Retrievable> retrievableIds;

    public Retrievable(Conjunction conjunction) {
        super(conjunction);
        this.retrievableIds = iterate(pattern().identifiers()).filter(Identifier::isRetrievable)
                .map(Identifier.Variable::asRetrievable).toSet();
    }

    public static Set<Retrievable> extractFrom(Conjunction conjunction, Set<Concludable> toExclude) {
        return grakn.core.logic.resolvable.Retrievable.Extractor.of(conjunction, toExclude).extract();
    }

    @Override
    public Optional<ThingVariable> generating() {
        return Optional.empty();
    }

    @Override
    public Set<Identifier.Variable.Retrievable> retrieves() {
        return retrievableIds;
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
        private final Set<Concludable> concludables;
        private final Set<SubgraphRegistry> subgraphs = new HashSet<>();
        private final Set<Variable> extractedVariables = new HashSet<>();
        private final Set<Constraint> extractedConstraints = new HashSet<>();

        public Extractor(Conjunction conjunction, Set<Concludable> concludables) {
            this.conjunction = conjunction;
            this.concludables = concludables;
        }

        public static Extractor of(Conjunction conjunction, Set<Concludable> concludables) {
            return new Extractor(conjunction, concludables);
        }

        public Set<Retrievable> extract() {
            concludables.forEach(concludable ->
                                         iterate(concludable.concludableConstraints())
                                                 .filter(constraint -> !(constraint.isType() && constraint.asType().isLabel() && constraint.asType().owner().id().isLabel())
                                                 ).forEachRemaining(extractedConstraints::add)
            );
            iterate(conjunction.variables()).filter(var -> var.id().isRetrievable()).forEachRemaining(var -> {
                if (!extractedVariables.contains(var)) {
                    SubgraphRegistry subgraph = new SubgraphRegistry();
                    subgraph.registerVariable(var);
                    subgraphs.add(subgraph);
                    extractedVariables.addAll(subgraph.registeredVariables());
                    iterate(subgraph.registeredConstraints).filter(constraint ->
                                                                           !(constraint.isType() && constraint.asType().isLabel() && constraint.asType().owner().id().isLabel())
                    ).forEachRemaining(extractedConstraints::add);
                }
            });
            return iterate(subgraphs).filter(SubgraphRegistry::isValid).map(subgraph -> {
                Set<TypeConstraint> labelConstraints = iterate(subgraph.registeredConstraints)
                        .filter(Constraint::isType).map(Constraint::asType).filter(TypeConstraint::isLabel).toSet();
                Set<? extends Constraint> otherConstraints = new HashSet<>(subgraph.registeredConstraints);
                otherConstraints.removeAll(labelConstraints);
                Conjunction.Cloner cloner = Conjunction.Cloner.cloneExactly(labelConstraints, otherConstraints);
                return new Retrievable(cloner.conjunction());
            }).toSet();
        }

        private class SubgraphRegistry {

            private final Set<Variable> registeredVariables = new HashSet<>();
            private final Set<Constraint> registeredConstraints = new HashSet<>();

            Set<Variable> registeredVariables() {
                return registeredVariables;
            }

            private boolean isValid() {
                return registeredConstraints.size() > 0;
            }

            private void registerVariable(Variable variable) {
                if (!registeredVariables.contains(variable)) {
                    registeredVariables.add(variable);
                    variable.constraints().forEach(this::registerConstraint);
                    variable.constraining().forEach(this::registerConstraint);
                }
            }

            private void registerConstraint(Constraint constraint) {
                if (constraint.isThing()) registerConstraint(constraint.asThing());
                else if (constraint.isType()) registerConstraint(constraint.asType());
                else throw GraknException.of(ILLEGAL_STATE);
            }

            private void registerConstraint(ThingConstraint thingConstraint) {
                if (!extractedConstraints.contains(thingConstraint)) {
                    registeredConstraints.add(thingConstraint);
                    iterate(thingConstraint.variables()).forEachRemaining(this::registerVariable);
                }
            }

            private void registerConstraint(TypeConstraint typeConstraint) {
                if (!extractedConstraints.contains(typeConstraint)) {
                    registeredConstraints.add(typeConstraint);
                    iterate(typeConstraint.variables()).forEachRemaining(this::registerVariable);
                }
            }
        }
    }
}
