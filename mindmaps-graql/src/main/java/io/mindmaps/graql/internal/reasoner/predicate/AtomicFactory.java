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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.admin.Conjunction;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.admin.VarProperty;
import io.mindmaps.graql.internal.pattern.VarInternal;
import io.mindmaps.graql.internal.pattern.property.HasResourceProperty;
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
        else if (!var.getValueEqualsPredicates().isEmpty() || var.getId().isPresent())
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
        else if (!var.getValueEqualsPredicates().isEmpty() || var.getId().isPresent())
            return new Substitution(var, parent);
        else
            return new Atom(var, parent);
    }

    public static Set<Atomic> createAtomSet(Conjunction<PatternAdmin> pat, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        MindmapsGraph graph = parent.getGraph().orElse(null);

        Set<VarAdmin> vars = pat.getVars();
        vars.forEach(var -> {
            if (var.getProperties(VarProperty.class).count() > 1 && !var.isRelation()) {
                String name = var.getName();
                String id = var.getId().orElse("");
                VarAdmin typeVar = var.getType().orElse(null);
                String type = typeVar != null? typeVar.getId().orElse("") : "";

                if (typeVar != null){
                    VarAdmin tVar = Graql.var(name).isa(type).admin();
                    atoms.add(AtomicFactory.create(tVar, parent));
                }
                if (!id.isEmpty()) atoms.add(new Substitution(name, graph.getEntity(id), parent));

                //value equals predicates
                var.getValueEqualsPredicates().forEach(eqPred -> {
                    VarAdmin valueVar = Graql.var(name).value(eqPred).admin();
                    Atomic atom = AtomicFactory.create(valueVar, parent);
                    atoms.add(atom);
                });

                //resources
                var.getProperties(HasResourceProperty.class).forEach(res -> {
                    String resType = res.getType();
                    res.getResource().getValuePredicates().forEach( pred -> {
                        VarAdmin resVar = Graql.var(name).admin();
                        resVar.has(resType, pred);
                        atoms.add(AtomicFactory.create(resVar, parent));
                    });

                    //res val as a variable
                    if(res.getResource().getValuePredicates().isEmpty()){
                        VarAdmin resVar = Graql.var(name).has(resType, Graql.var(res.getName())).admin();
                        atoms.add(AtomicFactory.create(resVar, parent));
                    }
                });
            }
            else
                atoms.add(AtomicFactory.create(var, parent));
        });

        return atoms;
    }

    public static Atomic create(Atomic atom) {
        return atom.clone();
    }
}
