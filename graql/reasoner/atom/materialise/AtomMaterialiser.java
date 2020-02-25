package grakn.core.graql.reasoner.atom.materialise;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.Atom;
import java.util.stream.Stream;

public interface AtomMaterialiser<T extends Atom> {

    Stream<ConceptMap> materialise(T toMaterialise);
}
