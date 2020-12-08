package grakn.core.reasoner.resolution;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.pattern.variable.Variable;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Unifier {

    public static Unifier identity() {
        return new Unifier(); // TODO A unifier that performs trivial mapping
    }

    public static Unifier identity(Set<Variable> vars) {
        return fromVariableMapping(vars.stream().collect(Collectors.toMap(Function.identity(), Function.identity())));
    }

    public static Unifier fromVariableMapping(Map<Variable, Variable> map) {
        return new Unifier(); // TODO Create a unifier from a 1:1 variable mapping from an alpha equivalence. Perhaps unnecessary if Unifier and the Variable Mapping implement the same interface
    }

    public ConceptMap unify(ConceptMap conceptMap) {
        return null; // TODO
    }

    public ConceptMap unUnify(ConceptMap unified) {
        return null; // TODO
    }
}
