package com.vaticle.typedb.core.reasoner.planner;

import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
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

    public abstract ReasonerPlanner.Plan<PLANELEMENT> callPlannerFunction(ReasonerPlanner reasonerPlanner);

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
        public ReasonerPlanner.Plan<Resolvable<?>> callPlannerFunction(ReasonerPlanner reasonerPlanner) {
            return reasonerPlanner.planConjunction(getConjunction(), bounds);
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
