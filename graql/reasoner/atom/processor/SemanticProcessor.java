package grakn.core.graql.reasoner.atom.processor;

import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.cache.SemanticDifference;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;

public interface SemanticProcessor<T extends Atom> {

    Unifier getUnifier(T childAtom, Atom parentAtom, UnifierType unifierType);

    MultiUnifier getMultiUnifier(T childAtom, Atom parentAtom, UnifierType unifierType);

    /**
     * Calculates the semantic difference between the this (parent) and child atom,
     * that needs to be applied on A(P) to find the subset belonging to A(C).
     *
     * @param childAtom child atom
     * @param unifier    parent->child unifier
     * @return semantic difference between this and child defined in terms of this variables
     */
    SemanticDifference semanticDifference(T parentAtom, Atom childAtom, Unifier unifier);

}
