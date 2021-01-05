package grakn.core.concept.answer;

import grakn.core.concept.Concept;

import java.util.List;

public class ConceptMapGroup implements Answer {
    private final Concept owner;
    private final List<ConceptMap> conceptMaps;

    public ConceptMapGroup(Concept owner, List<ConceptMap> conceptMaps) {
        this.owner = owner;
        this.conceptMaps = conceptMaps;
    }

    public Concept owner() {
        return this.owner;
    }

    public List<ConceptMap> conceptMaps() {
        return this.conceptMaps;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        final ConceptMapGroup a2 = (ConceptMapGroup) obj;
        return this.owner.equals(a2.owner) &&
                this.conceptMaps.equals(a2.conceptMaps);
    }

    @Override
    public int hashCode() {
        int hash = owner.hashCode();
        hash = 31 * hash + conceptMaps.hashCode();

        return hash;
    }
}
