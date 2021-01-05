package grakn.core.concept.answer;

import grakn.core.concept.Concept;

public class NumericGroup implements Answer {
    private final Concept owner;
    private final Numeric answers;

    public NumericGroup(Concept owner, Numeric answers) {
        this.owner = owner;
        this.answers = answers;
    }

    public Concept owner() {
        return this.owner;
    }

    public Numeric numeric() {
        return this.answers;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        final NumericGroup a2 = (NumericGroup) obj;
        return this.owner.equals(a2.owner) &&
                this.answers.equals(a2.answers);
    }

    @Override
    public int hashCode() {
        int hash = owner.hashCode();
        hash = 31 * hash + answers.hashCode();

        return hash;
    }
}
