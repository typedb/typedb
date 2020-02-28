package grakn.core.graql.reasoner.atom.task.convert;

import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;

public interface AtomConverter<T extends Atom> {

    RelationAtom toRelationAtom(T atom);

    AttributeAtom toAttributeAtom(T atom);

    IsaAtom toIsaAtom(T atom);
}
