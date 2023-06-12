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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public class ConjunctionStreamPlan {

    private final Set<Retrievable> extendOutputWith; // The variables in mergeWithRemainingVars
    private final Set<Retrievable> identifierVariables; // If the identifier variables match, the results will match
    private final Set<Retrievable> outputVariables;  // Strip out everything other than these.

    public ConjunctionStreamPlan(Set<Retrievable> extendOutputWith, Set<Retrievable> identifierVariables, Set<Retrievable> outputVariables) {
        this.extendOutputWith = extendOutputWith;
        this.identifierVariables = identifierVariables;
        this.outputVariables = outputVariables;
    }

    public static ConjunctionStreamPlan createConjunctionStreamPlan(List<Resolvable<?>> resolvableOrder, Set<Retrievable> inputVariables, Set<Retrievable> outputVariables) {
        // Set<Variable.Retrievable> extendWithVars = iterate(mergedPacket.concepts().keySet()).filter(v -> !remainingVariables.contains(v) && outputVariables.contains(v)).toSet();

        return null; // TODO
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

        public ResolvablePlan(Resolvable<?> resolvable, Set<Retrievable> extendOutputWith, Set<Retrievable> identifierVariables, Set<Retrievable> outputVariables) {
            super(extendOutputWith, identifierVariables, outputVariables);
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
    }

    public static class CompoundStreamPlan extends ConjunctionStreamPlan {
        private final List<ConjunctionStreamPlan> subPlans;

        private CompoundStreamPlan(List<ConjunctionStreamPlan> subPlans, Set<Retrievable> extendOutputWith, Set<Retrievable> identifierVariables, Set<Retrievable> outputVariables) {
            super(extendOutputWith, identifierVariables, outputVariables);
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

        public ConjunctionStreamPlan ithBranch(int i) {
            return subPlans.get(i);
        }

        public int size() {
            return subPlans.size();
        }
    }
}
