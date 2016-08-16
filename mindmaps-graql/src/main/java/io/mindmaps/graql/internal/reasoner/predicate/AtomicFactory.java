/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.reasoner.predicate;

import io.mindmaps.graql.internal.admin.PatternAdmin;
import io.mindmaps.graql.internal.admin.VarAdmin;
import io.mindmaps.graql.internal.reasoner.container.Query;

public class AtomicFactory {

    public static Atomic create(PatternAdmin pattern)
    {
        if (!pattern.isVar() )
            throw new IllegalArgumentException("Attempted to create an atom from pattern that is not a var: " + pattern.toString());

        VarAdmin var = pattern.asVar();
        if(var.isRelation())
            return new RelationAtom(var);
        else
            return new Atom(var);
    }

    public static Atomic create(PatternAdmin pattern, Query parent)
    {
        if (!pattern.isVar() )
        throw new IllegalArgumentException("Attempted to create an atom from pattern that is not a var: " + pattern.toString());

        VarAdmin var = pattern.asVar();
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
