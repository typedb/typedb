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

import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.reasoner.container.Query;
import io.mindmaps.util.ErrorMessage;

public class AtomicFactory {

    public static Atomic create(PatternAdmin pattern) {
        if (!pattern.isVar() )
            throw new IllegalArgumentException(ErrorMessage.PATTERN_NOT_VAR.getMessage(pattern.toString()));

        VarAdmin var = pattern.asVar();
        if(var.isRelation())
            return new Relation(var);
        else if (!var.getValuePredicates().isEmpty() || var.getId().isPresent())
            return new Substitution(var);
        else
            return new Atom(var);
    }

    public static Atomic create(PatternAdmin pattern, Query parent) {
        if (!pattern.isVar() )
        throw new IllegalArgumentException(ErrorMessage.PATTERN_NOT_VAR.getMessage(pattern.toString()));

        VarAdmin var = pattern.asVar();
        if(var.isRelation())
            return new Relation(var, parent);
        else if (!var.getValuePredicates().isEmpty() || var.getId().isPresent())
            return new Substitution(var, parent);
        else
            return new Atom(var, parent);
    }

    public static Atomic create(Atomic atom) {
        if(atom.isRelation())
            return new Relation((Relation) atom);
        else if (atom.isValuePredicate())
            return new Substitution((Substitution) atom);
        else return new Atom((Atom)atom);
    }
}
