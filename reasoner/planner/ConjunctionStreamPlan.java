/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.reasoner.planner;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class ConjunctionStreamPlan {
    protected final Set<Retrievable> identifierVariables; // If the identifier variables match, the results will match
    protected final Set<Retrievable> extendOutputWith; // The variables in mergeWithRemainingVars
    protected final Set<Retrievable> outputVariables;  // Strip out everything other than these.

    public ConjunctionStreamPlan(Set<Retrievable> identifierVariables, Set<Retrievable> extendOutputWith, Set<Retrievable> outputVariables) {
        this.identifierVariables = identifierVariables;
        this.extendOutputWith = extendOutputWith;
        this.outputVariables = outputVariables;
    }

    public static ConjunctionStreamPlan createFlattenedConjunctionStreamPlan(List<Resolvable<?>> resolvableOrder, Set<Retrievable> inputVariables, Set<Retrievable> outputVariables) {
        Builder builder = new Builder(resolvableOrder, inputVariables, outputVariables);
        ConjunctionStreamPlan unflattened = builder.build();
        ConjunctionStreamPlan flattened = builder.flatten(unflattened);
        return flattened;
    }

    public static boolean isExclusiveReader(ConjunctionStreamPlan.CompoundStreamPlan conjunctionStreamPlan, ConjunctionStreamPlan.CompoundStreamPlan unflattenedNextPlan, Set<Retrievable> topLevelBounds) {
        return unflattenedNextPlan.extendOutputWithVariables().isEmpty() && unflattenedNextPlan.identifierVariables.containsAll(Builder.VariableSets.difference(conjunctionStreamPlan.identifierVariables, topLevelBounds));
    }

    public static boolean mayProduceDuplicates(CompoundStreamPlan toSpawn) {
        return true; // TODO
    }

    public boolean isResolvable() {
        return false;
    }

    public boolean isCompoundStream() {
        return false;
    }

    public ResolvablePlan asResolvablePlan() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), className(ResolvablePlan.class));
    }

    public CompoundStreamPlan asCompoundStreamPlan() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), className(CompoundStreamPlan.class));
    }

    public Set<Retrievable> outputVariables() {
        return outputVariables;
    }

    public Set<Retrievable> identifierVariables() {
        return identifierVariables;
    }

    public Set<Retrievable> extendOutputWithVariables() {
        return extendOutputWith;
    }

    public static class ResolvablePlan extends ConjunctionStreamPlan {
        private final Resolvable<?> resolvable;

        public ResolvablePlan(Resolvable<?> resolvable, Set<Retrievable> identifierVariables, Set<Retrievable> extendOutputWith, Set<Retrievable> outputVariables) {
            super(identifierVariables, extendOutputWith, outputVariables);
            this.resolvable = resolvable;
        }

        @Override
        public boolean isResolvable() {
            return true;
        }

        @Override
        public ResolvablePlan asResolvablePlan() {
            return this;
        }

        public Resolvable<?> resolvable() {
            return resolvable;
        }

        @Override
        public String toString() {
            return String.format("{[(%s), (%s), (%s)] :: Resolvable(%s)}",
                    String.join(", ", iterate(identifierVariables).map(Identifier.Variable::toString).toList()),
                    String.join(", ", iterate(extendOutputWith).map(Identifier.Variable::toString).toList()),
                    String.join(", ", iterate(outputVariables).map(Identifier.Variable::toString).toList()),
                    resolvable.toString()
            );
        }
    }

    public static class CompoundStreamPlan extends ConjunctionStreamPlan {
        private final List<ConjunctionStreamPlan> subPlans;

        public CompoundStreamPlan(List<ConjunctionStreamPlan> subPlans,
                                  Set<Retrievable> identifierVariables, Set<Retrievable> extendOutputWith, Set<Retrievable> outputVariables) {
            super(identifierVariables, extendOutputWith, outputVariables);
            assert subPlans.size() > 1;
            this.subPlans = subPlans;
        }

        @Override
        public boolean isCompoundStream() {
            return true;
        }

        @Override
        public CompoundStreamPlan asCompoundStreamPlan() {
            return this;
        }

        public ConjunctionStreamPlan ithChild(int i) {
            return subPlans.get(i);
        }

        public int size() {
            return subPlans.size();
        }

        @Override
        public String toString() {
            return String.format("{[(%s), (%s), (%s)] :: [%s]}",
                    String.join(", ", iterate(identifierVariables).map(Identifier.Variable::toString).toList()),
                    String.join(", ", iterate(extendOutputWith).map(Identifier.Variable::toString).toList()),
                    String.join(", ", iterate(outputVariables).map(Identifier.Variable::toString).toList()),
                    String.join(" ; ", iterate(subPlans).map(ConjunctionStreamPlan::toString).toList()));
        }
    }

    private static class Builder {
        private final List<Resolvable<?>> resolvables;
        private final Set<Retrievable> inputBounds;
        private final Set<Retrievable> outputBounds;

        private final List<Set<Retrievable>> boundsAt;

        private Builder(List<Resolvable<?>> resolvables, Set<Retrievable> inputBounds, Set<Retrievable> outputBounds) {
            this.resolvables = resolvables;
            this.inputBounds = inputBounds;
            this.outputBounds = outputBounds;
            boundsAt = new ArrayList<>();
            Set<Retrievable> runningBounds = new HashSet<>(inputBounds);
            for (Resolvable<?> resolvable : resolvables) {
                boundsAt.add(new HashSet<>(runningBounds));
                iterate(resolvable.retrieves()).filter(Retrievable::isName).forEachRemaining(runningBounds::add);
            }
        }

        private ConjunctionStreamPlan build() {
            return buildRecursivePrefix(resolvables, inputBounds, outputBounds);
        }

        public ConjunctionStreamPlan buildRecursivePrefix(List<Resolvable<?>> subConjunction, Set<Retrievable> availableInputs, Set<Retrievable> requiredOutputs) {
            if (subConjunction.size() == 1) {
                VariableSets variableSets = VariableSets.determineVariableSets(list(), subConjunction, availableInputs, requiredOutputs);
                //  use resolvableOutputs instead of rightOutputs because this node has to do the job of the parent as well - joining the identifiers
                Set<Retrievable> resolvableOutputs = VariableSets.difference(requiredOutputs, variableSets.extendOutputWith);
                return new ResolvablePlan(subConjunction.get(0), variableSets.rightInputs, variableSets.extendOutputWith, resolvableOutputs);
            } else {
                Pair<List<Resolvable<?>>, List<Resolvable<?>>> divided = divide(subConjunction);
                VariableSets variableSets = VariableSets.determineVariableSets(divided.first(), divided.second(), availableInputs, requiredOutputs);
                ConjunctionStreamPlan leftPlan = buildRecursivePrefix(divided.first(), variableSets.leftIdentifiers, variableSets.leftOutputs);
                ConjunctionStreamPlan rightPlan = buildRecursiveSuffix(divided.second(), variableSets.rightInputs, variableSets.rightOutputs);

                return new CompoundStreamPlan(list(leftPlan, rightPlan), variableSets.identifiers, variableSets.extendOutputWith, requiredOutputs);
            }
        }

        public ConjunctionStreamPlan buildRecursiveSuffix(List<Resolvable<?>> subConjunction, Set<Retrievable> availableInputs, Set<Retrievable> requiredOutputs) {
            // We should be able to call buildRecursivePrefix and get the same answer.
            // If we did everything right, we should have an easy build.
            if (subConjunction.size() == 1) {
                VariableSets variableSets = VariableSets.determineVariableSets(list(), subConjunction, availableInputs, requiredOutputs);
                //  use resolvableOutputs instead of rightOutputs because this node has to do the job of the parent as well - joining the identifiers
                Set<Retrievable> resolvableOutputs = VariableSets.difference(requiredOutputs, variableSets.extendOutputWith);
                return new ResolvablePlan(subConjunction.get(0), variableSets.rightInputs, variableSets.extendOutputWith, resolvableOutputs);
            } else {
                List<Resolvable<?>> suffix = subConjunction.subList(1, subConjunction.size());
                VariableSets variableSets = VariableSets.determineVariableSets(subConjunction.subList(0, 1), subConjunction.subList(1, subConjunction.size()), availableInputs, requiredOutputs);
                ConjunctionStreamPlan leftPlan = new ResolvablePlan(subConjunction.get(0), variableSets.leftIdentifiers, set(), variableSets.leftOutputs);
                ConjunctionStreamPlan rightPlan = buildRecursiveSuffix(suffix, variableSets.rightInputs, variableSets.rightOutputs);
                return new CompoundStreamPlan(list(leftPlan, rightPlan),
                        variableSets.identifiers, variableSets.extendOutputWith, requiredOutputs);
            }
        }

        public Pair<List<Resolvable<?>>, List<Resolvable<?>>> divide(List<Resolvable<?>> subConjunction) {
            int splitAfter = findSplitAfterIndex(subConjunction);
            List<Resolvable<?>> left = new ArrayList<>();
            List<Resolvable<?>> right = new ArrayList<>();
            for (int j = 0; j < subConjunction.size(); j++) {
                if (j <= splitAfter) left.add(subConjunction.get(j));
                else right.add(subConjunction.get(j));
            }

            return new Pair<>(left, right);
        }

        private int findSplitAfterIndex(List<Resolvable<?>> subConjunction) {
            Set<Retrievable> nextSuffixVariables = new HashSet<>(subConjunction.get(subConjunction.size() - 1).retrieves());
            int r;
            for (r = subConjunction.size() - 2; r > 0; r--) { // r > 0 because we need atleast one thing in our left
                nextSuffixVariables.addAll(subConjunction.get(r).retrieves());
                Set<Retrievable> subtreeBounds = VariableSets.intersection(boundsAt.get(r), nextSuffixVariables);
                Set<Retrievable> candidateFirstChildBounds = VariableSets.intersection(subConjunction.get(r).retrieves(), boundsAt.get(r));

                if (!VariableSets.difference(subtreeBounds, candidateFirstChildBounds).isEmpty()) {
                    break;
                }
            }
            return r;
        }

        private ConjunctionStreamPlan flatten(ConjunctionStreamPlan conjunctionStreamPlan) {
            if (conjunctionStreamPlan.isResolvable()) {
                return conjunctionStreamPlan;
            } else {
                CompoundStreamPlan compoundStreamPlan = conjunctionStreamPlan.asCompoundStreamPlan();
                assert compoundStreamPlan.size() == 2;
                List<ConjunctionStreamPlan> subPlans = new ArrayList<>();
                for (int i = 0; i < 2; i++) {
                    ConjunctionStreamPlan unflattenedChild = compoundStreamPlan.ithChild(i);

                    if (canFlattenInto(compoundStreamPlan, unflattenedChild)) {
                        subPlans.addAll(flatten(unflattenedChild).asCompoundStreamPlan().subPlans);
                    } else {
                        subPlans.add(flatten(unflattenedChild));
                    }
                }
                return new CompoundStreamPlan(subPlans, compoundStreamPlan.identifierVariables(), compoundStreamPlan.extendOutputWithVariables(), compoundStreamPlan.outputVariables());
            }
        }

        private boolean canFlattenInto(ConjunctionStreamPlan.CompoundStreamPlan conjunctionStreamPlan, ConjunctionStreamPlan unflattenedChild) {
            return unflattenedChild.isCompoundStream() &&
                    isExclusiveReader(conjunctionStreamPlan.asCompoundStreamPlan(), unflattenedChild.asCompoundStreamPlan(), inputBounds) &&
                    rightChildBoundsSatisfied(conjunctionStreamPlan.asCompoundStreamPlan(), unflattenedChild.asCompoundStreamPlan());
        }

        private static boolean rightChildBoundsSatisfied(ConjunctionStreamPlan.CompoundStreamPlan conjunctionStreamPlan, ConjunctionStreamPlan.CompoundStreamPlan unflattenedNextPlan) {
            return VariableSets.difference(
                            unflattenedNextPlan.ithChild(1).identifierVariables,
                            VariableSets.union(conjunctionStreamPlan.identifierVariables, unflattenedNextPlan.ithChild(0).outputVariables))
                    .isEmpty();
        }

        private static class VariableSets {

            public final Set<Retrievable> identifiers;
            public final Set<Retrievable> extendOutputWith;
            public final Set<Retrievable> requiredOutputs;
            public final Set<Retrievable> leftIdentifiers;
            public final Set<Retrievable> leftOutputs;
            public final Set<Retrievable> rightInputs;
            public final Set<Retrievable> rightOutputs;

            public VariableSets(Set<Retrievable> identifiers, Set<Retrievable> extendOutputWith, Set<Retrievable> requiredOutputs,
                                Set<Retrievable> leftIdentifiers, Set<Retrievable> leftOutputs,
                                Set<Retrievable> rightInputs, Set<Retrievable> rightOutputs) {
                this.identifiers = identifiers;
                this.extendOutputWith = extendOutputWith;
                this.requiredOutputs = requiredOutputs;
                this.leftIdentifiers = leftIdentifiers;
                this.leftOutputs = leftOutputs;
                this.rightInputs = rightInputs;
                this.rightOutputs = rightOutputs;
            }

            private static VariableSets determineVariableSets(List<Resolvable<?>> left, List<Resolvable<?>> right, Set<Retrievable> availableInputs, Set<Retrievable> requiredOutputs) {
                Set<Retrievable> leftVariables = iterate(left).flatMap(resolvable -> iterate(resolvable.retrieves())).filter(Retrievable::isName).toSet();
                Set<Retrievable> rightVariables = iterate(right).flatMap(resolvable -> iterate(resolvable.retrieves())).filter(Retrievable::isName).toSet();
                Set<Retrievable> allUsedVariables = union(leftVariables, rightVariables);

                Set<Retrievable> identifiers = intersection(availableInputs, allUsedVariables);
                Set<Retrievable> extendOutputWith = difference(availableInputs, allUsedVariables);
                Set<Retrievable> rightOutputs = difference(requiredOutputs, availableInputs);

                Set<Retrievable> leftIdentifiers = intersection(identifiers, leftVariables);
                Set<Retrievable> rightInputs = intersection(union(identifiers, leftVariables), union(rightVariables, rightOutputs));
                Set<Retrievable> leftOutputs = difference(rightInputs, difference(identifiers, leftIdentifiers));

                return new VariableSets(identifiers, extendOutputWith, requiredOutputs, leftIdentifiers, leftOutputs, rightInputs, rightOutputs);
            }

            public static Set<Retrievable> union(Set<Retrievable> a, Set<Retrievable> b) {
                Set<Retrievable> result = new HashSet<>(a);
                result.addAll(b);
                return result;
            }

            private static Set<Retrievable> intersection(Set<Retrievable> a, Set<Retrievable> b) {
                Set<Retrievable> result = new HashSet<>(a);
                result.retainAll(b);
                return result;
            }

            private static Set<Retrievable> difference(Set<Retrievable> a, Set<Retrievable> b) {
                Set<Retrievable> result = new HashSet<>(a);
                result.removeAll(b);
                return result;
            }
        }
    }
}
