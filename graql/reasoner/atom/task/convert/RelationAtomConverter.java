package grakn.core.graql.reasoner.atom.task.convert;

import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;

public class RelationAtomConverter implements AtomConverter<RelationAtom> {
    @Override
    public RelationAtom toRelationAtom(RelationAtom atom) {
        return null;
    }

    @Override
    public AttributeAtom toAttributeAtom(RelationAtom atom) {
        return null;
    }

    @Override
    public IsaAtom toIsaAtom(RelationAtom atom) {
        return null;
    }
}
