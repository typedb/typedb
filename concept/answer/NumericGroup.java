package grakn.core.concept.answer;

import grakn.core.concept.Concept;

public class NumericGroup implements Answer {
    private final Concept owner;
    private final Numeric numeric;

    public NumericGroup(Concept owner, Numeric numeric) {
        this.owner = owner;
        this.numeric = numeric;
    }

    public Concept owner() {
        return this.owner;
    }

    public Numeric numeric() {
        return this.numeric;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        final NumericGroup a2 = (NumericGroup) obj;
        return this.owner.equals(a2.owner) &&
                this.numeric.equals(a2.numeric);
    }

    @Override
    public int hashCode() {
        int hash = owner.hashCode();
        hash = 31 * hash + numeric.hashCode();

        return hash;
    }
}
