package io.mindmaps.reasoner.internal.predicate;

import io.mindmaps.graql.api.query.Pattern;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.reasoner.internal.container.Query;

public class AtomicFactory {

    public static Atomic create(Pattern.Admin pattern)
    {
        if (!pattern.isVar() )
            throw new IllegalArgumentException("Attempted to create an atom from pattern that is not a var: " + pattern.toString());

        Var.Admin var = pattern.asVar();
        if(var.isRelation())
            return new RelationAtom(var);
        else
            return new Atom(var);
    }

    public static Atomic create(Pattern.Admin pattern, Query parent)
    {
        if (!pattern.isVar() )
        throw new IllegalArgumentException("Attempted to create an atom from pattern that is not a var: " + pattern.toString());

        Var.Admin var = pattern.asVar();
        if(var.isRelation())
            return new RelationAtom(var, parent);
        else
            return new Atom(var, parent);
    }

    public static Atomic create(Atomic atom) {
        if(atom.isRelation()) return new RelationAtom((RelationAtom) atom);
        else return new Atom((Atom)atom);
    }

}
