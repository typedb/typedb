package grakn.core.reasoner.resolution;

import grakn.core.pattern.variable.Variable;

import java.util.HashMap;
import java.util.Set;

public class Unifier {

    public static Unifier identity() {
        return null; // TODO A unifier that performs trivial mapping
    }

    public static Unifier identity(Set<Variable> vars) {
        return null; // TODO A trivial mapping for the given variables
    }

    public static Unifier fromVariableMapping(HashMap<Variable, Variable> map) {
        return null; // TODO Create a unifier from a 1:1 variable mapping from an alpha equivalence. Perhaps unnecessary if Unifier and the Variable Mapping implement the same interface
    }
}
