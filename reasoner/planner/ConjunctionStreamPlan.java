// I'm just breaking checkstyle so I know I've implemented it but not manually verified it.

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

import com.vaticle.typedb.common.collection.Collections;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        ConjunctionStreamPlan flattened = flatten(createBinaryConjunctionStreamPlan(resolvableOrder, inputVariables, outputVariables));
        return flattened;
    }

    private static ConjunctionStreamPlan createBinaryConjunctionStreamPlan(List<Resolvable<?>> resolvableOrder, Set<Retrievable> inputVariables, Set<Retrievable> outputVariables) {
        Set<Retrievable> conjunctionVariables = iterate(resolvableOrder).flatMap(resolvable -> iterate(resolvable.retrieves())).toSet();
        Set<Retrievable> identifiers = intersection(inputVariables, conjunctionVariables);
        // assert resolvableOrder.get(0).retrieves().containsAll(identifiers); // TODO: Remove: not true for first level
        Set<Retrievable> joinOutputs = difference(inputVariables, identifiers);
        if (resolvableOrder.size() == 1) {
            Set<Retrievable> resolvableOutputs = difference(outputVariables, joinOutputs);
            return new ResolvablePlan(resolvableOrder.get(0), identifiers, joinOutputs, resolvableOutputs);
        }

        Resolvable<?> first = resolvableOrder.get(0);

        // Find longest suffix which won't have redundant bounds
        Set<Retrievable> suffixVariables = new HashSet<>();
        List<Resolvable<?>> left, right;
        {
            int i;
            for (i = resolvableOrder.size() - 1; i >= 1; i--) {
                suffixVariables.addAll(resolvableOrder.get(i).retrieves());
                if (difference(intersection(first.retrieves(), suffixVariables), resolvableOrder.get(i).retrieves()).size() > 0) {
                    break;
                }
            }

            left = new ArrayList<>();
            right = new ArrayList<>();
            for (int j = 0; j < resolvableOrder.size(); j++) {
                if (j <= i) left.add(resolvableOrder.get(j));
                else right.add(resolvableOrder.get(j));
            }
        }

        assert !left.isEmpty() && !right.isEmpty();
        ConjunctionStreamPlan leftPlan, rightPlan;
        {
            Set<Retrievable> leftVariables = iterate(left).flatMap(l -> iterate(l.retrieves())).toSet();
            Set<Retrievable> rightVariables = iterate(right).flatMap(r -> iterate(r.retrieves())).toSet();
            // TODO: Check if this is true: If a variable is in the identifiers, it can be removed from the output of the children.
            Set<Retrievable> childrenOutput = difference(difference(outputVariables, identifiers), joinOutputs);

            Set<Retrievable> leftInputs = intersection(inputVariables, leftVariables);
            Set<Retrievable> leftOutputs = intersection(leftVariables, union(childrenOutput, rightVariables));
            Set<Retrievable> rightInputs = union(leftOutputs, intersection(inputVariables, rightVariables));
            // assert union(leftInputs, joinOutputs).equals(inputVariables); // TODO: Remove: Also not true for first level
            leftPlan = createBinaryConjunctionStreamPlan(left, leftInputs, leftOutputs);
            rightPlan = createBinaryConjunctionStreamPlan(right, rightInputs, childrenOutput);
        }

        return new CompoundStreamPlan(Collections.list(leftPlan, rightPlan), identifiers, joinOutputs, outputVariables);
    }

    private static ConjunctionStreamPlan flatten(ConjunctionStreamPlan conjunctionStreamPlan) {
        if (conjunctionStreamPlan.isResolvable()) {
            return conjunctionStreamPlan;
        } else {
            CompoundStreamPlan compoundStreamPlan = conjunctionStreamPlan.asCompoundStreamPlan();
            assert compoundStreamPlan.size() == 2;
            List<ConjunctionStreamPlan> subPlans = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                ConjunctionStreamPlan unflattenedNextPlan = compoundStreamPlan.ithChild(i);
                if (unflattenedNextPlan.isCompoundStream() && unflattenedNextPlan.asCompoundStreamPlan().canFlatten()) {
                    subPlans.addAll(flatten(unflattenedNextPlan).asCompoundStreamPlan().subPlans);
                } else {
                    subPlans.add(flatten(unflattenedNextPlan));
                }
            }

            return new CompoundStreamPlan(subPlans, compoundStreamPlan.identifierVariables(), compoundStreamPlan.extendOutputWithVariables(), compoundStreamPlan.outputVariables());
        }
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
                    String.join(", ", iterate(identifierVariables).map(v -> v.toString()).toList()),
                    String.join(", ", iterate(extendOutputWith).map(v -> v.toString()).toList()),
                    String.join(", ", iterate(outputVariables).map(v -> v.toString()).toList()),
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

        public boolean canFlatten() {
            return extendOutputWith.isEmpty() &&
                    ConjunctionStreamPlan.difference(ConjunctionStreamPlan.intersection(identifierVariables, subPlans.get(1).identifierVariables), subPlans.get(0).identifierVariables).isEmpty();
        }

        public int size() {
            return subPlans.size();
        }

        @Override
        public String toString() {
            return String.format("{[(%s), (%s), (%s)] :: [%s]}",
                    String.join(", ", iterate(identifierVariables).map(v -> v.toString()).toList()),
                    String.join(", ", iterate(extendOutputWith).map(v -> v.toString()).toList()),
                    String.join(", ", iterate(outputVariables).map(v -> v.toString()).toList()),
                    String.join(" ; ", iterate(subPlans).map(ConjunctionStreamPlan::toString).toList()));
        }
    }
}
