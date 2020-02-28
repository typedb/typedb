package grakn.core.graql.reasoner.atom.task.convert;

import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;

public interface AtomConverter<T extends Atom> {

    RelationAtom toRelationAtom(T atom, ReasoningContext ctx);

    AttributeAtom toAttributeAtom(T atom, ReasoningContext ctx);

    IsaAtom toIsaAtom(T atom, ReasoningContext ctx);
}
