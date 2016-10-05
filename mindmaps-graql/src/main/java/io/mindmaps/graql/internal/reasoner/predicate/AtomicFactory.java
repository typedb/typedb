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

import io.mindmaps.graql.Graql;
import io.mindmaps.graql.admin.Conjunction;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.reasoner.query.Query;
import io.mindmaps.util.ErrorMessage;
import java.util.HashSet;
import java.util.Set;

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
        return atom.clone();
    }

    public static Set<Atomic> createAtomSet(Conjunction<PatternAdmin> pat, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        Set<VarAdmin> vars = pat.getVars();
        vars.forEach(var -> {
            if(var.getType().isPresent() && (var.getId().isPresent() || !var.getValueEqualsPredicates().isEmpty())) {
                VarAdmin typeVar = Graql.var(var.getName()).isa(var.getType().orElse(null)).admin();
                atoms.add(AtomicFactory.create(typeVar, parent));

                if (var.getId().isPresent()) {
                    VarAdmin sub = Graql.var(var.getName()).id(var.getId().orElse(null)).admin();
                    atoms.add(AtomicFactory.create(sub, parent));
                }
                else if (!var.getValueEqualsPredicates().isEmpty()){
                    if(var.getValueEqualsPredicates().size() > 1)
                        throw new IllegalArgumentException(ErrorMessage.MULTI_VALUE_VAR.getMessage(var.toString()));
                    VarAdmin sub = Graql.var(var.getName()).value(var.getValueEqualsPredicates().iterator().next()).admin();
                    atoms.add(AtomicFactory.create(sub, parent));
                }
            }
            else
                atoms.add(AtomicFactory.create(var, parent));
        });

        return atoms;
    }
}
