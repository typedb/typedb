package com.vaticle.typedb.core.reasoner.planner;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.Objects;
import java.util.Set;

public abstract class Plannable<IDTYPE, PLANELEMENT> {
    IDTYPE plannableIdentifier;
    Set<Identifier.Variable.Retrievable> bounds;

    private Plannable(IDTYPE plannableIdentifier, Set<Identifier.Variable.Retrievable> bounds) {
        this.plannableIdentifier = plannableIdentifier;
        this.bounds = bounds;
    }

    public abstract PlanSearch.Plan<PLANELEMENT> callPlannerFunction(PlanSearch planSearch);

    public static PlannableConjunction ofConjunction(ResolvableConjunction conjunction, Set<Identifier.Variable.Retrievable> bounds) {
        return new PlannableConjunction(conjunction, bounds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(plannableIdentifier, bounds);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        else if (other.getClass() != this.getClass()) {
            return false;
        }
        Plannable that = (Plannable) other;
        return this.plannableIdentifier.equals(that.plannableIdentifier) && this.bounds.equals(that.bounds);
    }

    static class PlannableConjunction extends Plannable<ResolvableConjunction, Resolvable<?>> {
        public PlannableConjunction(ResolvableConjunction plannableIdentifier, Set<Identifier.Variable.Retrievable> bounds) {
            super(plannableIdentifier, bounds);
        }

        @Override
        public PlanSearch.Plan<Resolvable<?>> callPlannerFunction(PlanSearch planSearch) {
            return planSearch.planConjunction(getConjunction(), bounds);
        }

        public ResolvableConjunction getConjunction() {
            return plannableIdentifier;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            else if (other.getClass() != this.getClass()) {
                return false;
            }
            Plannable that = (Plannable) other;
            // TODO: We don't trust conjunction.equals ; Update if that ever changes.
            return this.plannableIdentifier == that.plannableIdentifier && this.bounds.equals(that.bounds);
        }

    }
}
