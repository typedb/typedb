/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.util.ErrorMessage;
import java.util.HashSet;
import java.util.Set;

public class AtomicFactory {

    public static Atomic create(PatternAdmin pattern) {
        if (!pattern.isVar() )
            throw new IllegalArgumentException(ErrorMessage.PATTERN_NOT_VAR.getMessage(pattern.toString()));

        VarAdmin var = pattern.asVar();
        if(var.isRelation())
            return new Relation(var);
        else if(!var.getResourcePredicates().isEmpty())
            return new Resource(var);
        else if (var.getId().isPresent())
            return new IdPredicate(var);
        else if (!var.getValuePredicates().isEmpty())
            return new ValuePredicate(var);
        else
            return new TypeAtom(var);
    }

    public static Atomic create(PatternAdmin pattern, Query parent) {
        if (!pattern.isVar() )
            throw new IllegalArgumentException(ErrorMessage.PATTERN_NOT_VAR.getMessage(pattern.toString()));

        VarAdmin var = pattern.asVar();
        if(var.isRelation())
            return new Relation(var, parent);
        else if(!var.getResourcePredicates().isEmpty())
            return new Resource(var, parent);
        else if (var.getId().isPresent())
            return new IdPredicate(var, parent);
        else if (!var.getValuePredicates().isEmpty())
            return new ValuePredicate(var, parent);
        else
            return new TypeAtom(var, parent);
    }

    public static Atomic create(Atomic atom, Query parent) {
        Atomic copy = atom.clone();
        copy.setParentQuery(parent);
        return copy;
    }

    public static Set<Atomic> createAtomSet(Conjunction<PatternAdmin> pattern, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        Set<VarAdmin> vars = pattern.getVars();
        vars.stream().filter(VarAdmin::isRelation).forEach(var -> atoms.add(create(var, parent)));
        vars.stream()
                .filter(var -> !var.isRelation())
                .forEach(var -> var.getProperties()
                        .forEach(prop -> atoms.addAll(PropertyMapper.map(prop, var, parent))));
        return atoms;
    }
}
