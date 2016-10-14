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
import io.mindmaps.graql.internal.pattern.property.HasResourceProperty;
import io.mindmaps.graql.internal.reasoner.query.Query;
import io.mindmaps.util.ErrorMessage;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

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
            return new Substitution(var);
        else if (!var.getValuePredicates().isEmpty())
            return new ValuePredicate(var);
        else
            return new Type(var);
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
            return new Substitution(var, parent);
        else if (!var.getValuePredicates().isEmpty())
            return new ValuePredicate(var, parent);
        else
            return new Type(var, parent);
    }

    public static Atomic create(Atomic atom, Query parent) {
        Atomic copy = atom.clone();
        copy.setParentQuery(parent);
        return copy;
    }

    private static Set<Atomic> createResourceAtomSet(PatternAdmin pattern, Query parent){
        Set<Atomic> resourceAtoms = new HashSet<>();
        VarAdmin var = pattern.asVar();
        String varName = var.getName();
        var.getProperties(HasResourceProperty.class).forEach(res -> {
            String resType = res.getType();
            VarAdmin baseVar = res.getResource();
            String valueVariable = baseVar.isUserDefinedName()?
                    baseVar.getName() : varName + "-" + resType + "-" + UUID.randomUUID().toString();

            VarAdmin resVar = Graql.var(varName).has(resType, Graql.var(valueVariable)).admin();
            resourceAtoms.add(AtomicFactory.create(resVar, parent));

            //add value predicate
            baseVar.getValuePredicates().forEach(pred -> {
                    VarAdmin resourceValueVar = Graql.var(valueVariable).value(pred).admin();
                    resourceAtoms.add(AtomicFactory.create(resourceValueVar, parent));
            });
        });
        return resourceAtoms;
    }

    public static Set<Atomic> createAtomSet(Conjunction<PatternAdmin> pattern, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        MindmapsGraph graph = parent.getGraph().orElse(null);

        Set<VarAdmin> vars = pattern.getVars();
        vars.forEach(var -> {
            if (var.getProperties().count() > 1 && !var.isRelation() ) {
                String name = var.getName();
                String id = var.getId().orElse("");
                VarAdmin typeVar = var.getType().orElse(null);
                String type = typeVar != null? typeVar.getId().orElse("") : "";

                if (typeVar != null){
                    VarAdmin tVar = Graql.var(name).isa(type).admin();
                    atoms.add(AtomicFactory.create(tVar, parent));
                }
                if (!id.isEmpty())
                    atoms.add(new Substitution(name, graph.getEntity(id), parent));

                //value equals predicates
                var.getValuePredicates().forEach(pred -> {
                    VarAdmin valueVar = Graql.var(name).value(pred).admin();
                    Atomic atom = AtomicFactory.create(valueVar, parent);
                    atoms.add(atom);
                });

                //resources
                atoms.addAll(createResourceAtomSet(var, parent));

            }
            else {
                Set<Atomic> resourceAtomSet = createResourceAtomSet(var, parent);
                if (resourceAtomSet.isEmpty()) atoms.add(AtomicFactory.create(var, parent));
                else
                    atoms.addAll(resourceAtomSet);
            }
        });

        return atoms;
    }
}
