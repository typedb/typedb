package grakn.core.concept.answer;

import grakn.core.concept.Concept;

import java.util.List;

public class ConceptMapGroup implements Answer {
    private final Concept owner;
    private final List<ConceptMap> answers;

    public ConceptMapGroup(Concept owner, List<ConceptMap> answers) {
        this.owner = owner;
        this.answers = answers;
    }

    public Concept owner() {
        return this.owner;
    }

    public List<ConceptMap> answers() {
        return this.answers;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        final ConceptMapGroup a2 = (ConceptMapGroup) obj;
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
