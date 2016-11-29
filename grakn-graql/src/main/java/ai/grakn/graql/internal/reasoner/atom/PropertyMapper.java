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

import ai.grakn.GraknGraph;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.pattern.property.HasResourceTypeProperty;
import ai.grakn.graql.internal.pattern.property.IdProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.PlaysRoleProperty;
import ai.grakn.graql.internal.pattern.property.SubProperty;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.pattern.property.DataTypeProperty;
import ai.grakn.graql.internal.pattern.property.IsAbstractProperty;
import ai.grakn.graql.internal.pattern.property.RegexProperty;
import ai.grakn.graql.internal.pattern.property.NameProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.reasoner.atom.property.DataTypeAtom;
import ai.grakn.graql.internal.reasoner.atom.property.IsAbstractAtom;
import ai.grakn.graql.internal.reasoner.atom.property.RegexAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.atom.binary.Resource;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.util.ErrorMessage;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.reasoner.query.Query;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class PropertyMapper {

    public static Set<Atomic> map(VarProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph){
        if(prop instanceof RelationProperty)
            return map((RelationProperty)prop, var, vars, parent, graph);
        else if (prop instanceof IdProperty)
            return map((IdProperty)prop, var, vars, parent, graph);
        else if (prop instanceof NameProperty)
            return map((NameProperty)prop, var, vars, parent, graph);
        else if(prop instanceof ValueProperty)
            return map((ValueProperty)prop, var, vars, parent, graph);
        else if (prop instanceof SubProperty)
            return map((SubProperty)prop, var, vars, parent, graph);
        else if (prop instanceof PlaysRoleProperty)
            return map((PlaysRoleProperty)prop, var, vars, parent, graph);
        else if (prop instanceof HasResourceTypeProperty)
            return map((HasResourceTypeProperty)prop, var, vars, parent, graph);
        else if (prop instanceof IsaProperty)
            return map((IsaProperty)prop, var, vars, parent, graph);
        else if (prop instanceof HasResourceProperty)
            return map((HasResourceProperty)prop, var, vars, parent, graph);
        else if (prop instanceof IsAbstractProperty)
            return map((IsAbstractProperty)prop, var, vars, parent, graph);
        else if (prop instanceof DataTypeProperty)
            return map((DataTypeProperty)prop, var, vars, parent, graph);
        else if (prop instanceof RegexProperty)
            return map((RegexProperty)prop, var, vars, parent, graph);
        else
            throw new IllegalArgumentException(ErrorMessage.GRAQL_PROPERTY_NOT_MAPPED.getMessage(prop.toString()));
    }

    //TODO all these should eventually go into atom constructors

    private static Set<Atomic> map(RelationProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        Set<Atomic> atoms = new HashSet<>();
        Var relVar = var.isUserDefinedName()? Graql.var(var.getVarName()) : Graql.var();
        Set<RelationPlayer> relationPlayers = prop.getRelationPlayers().collect(Collectors.toSet());
        relationPlayers.forEach(rp -> {
            VarAdmin role = rp.getRoleType().orElse(null);
            VarAdmin rolePlayer = rp.getRolePlayer();
            if (role != null) relVar.rel(role, rolePlayer);
            else relVar.rel(rolePlayer);
        });

        //id part
        IsaProperty isaProp = var.getProperty(IsaProperty.class).orElse(null);
        IdPredicate predicate = null;
        //Isa present
        if (isaProp != null) {
            VarAdmin isaVar = isaProp.getType();
            String typeName = isaVar.getTypeName().orElse("");
            String typeVariable = typeName.isEmpty()? isaVar.getVarName() : "rel-" + UUID.randomUUID().toString();
            relVar.isa(Graql.var(typeVariable));
            if (!typeName.isEmpty()) {
                VarAdmin idVar = Graql.var(typeVariable)
                        .id(getTypeId(isaProp.getProperty(), graph)).admin();
                predicate = new IdPredicate(idVar, parent);
            }
            else
                predicate = getUserDefinedIdPredicate(typeVariable, vars, parent);
        }

        atoms.add(new Relation(relVar.admin(), predicate, parent));
        if (predicate != null) atoms.add(predicate);
        return atoms;
    }

    private static Set<Atomic> map(IdProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        return Sets.newHashSet(new IdPredicate(var.getVarName(), prop, parent));
    }

    private static Set<Atomic> map(NameProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        return Sets.newHashSet(new IdPredicate(var.getVarName(), prop, parent));
    }

    private static Set<Atomic> map(ValueProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        return Sets.newHashSet(new ValuePredicate(var.getVarName(), prop, parent));
    }

    private static Set<Atomic> map(RegexProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        return Sets.newHashSet(new RegexAtom(var.getVarName(), prop, parent));
    }

    private static Set<Atomic> map(DataTypeProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        return Sets.newHashSet(new DataTypeAtom(var.getVarName(), prop, parent));
    }

    private static Set<Atomic> map(IsAbstractProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        return Sets.newHashSet(new IsAbstractAtom(var.getVarName(), prop, parent));
    }

    private static Set<Atomic> map(SubProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getVarName();
        VarAdmin typeVar = prop.getSuperType();
        String typeVariable = typeVar.isUserDefinedName() ?
                typeVar.getVarName() : varName + "-sub-" + UUID.randomUUID().toString();
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent, graph);

        VarAdmin resVar = Graql.var(varName).sub(Graql.var(typeVariable)).admin();
        atoms.add(new TypeAtom(resVar, parent));
        if (predicate != null) atoms.add(predicate);
        return atoms;
    }

    private static Set<Atomic> map(PlaysRoleProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getVarName();
        VarAdmin typeVar = prop.getRole();
        String typeVariable = typeVar.isUserDefinedName() ?
                typeVar.getVarName() : varName + "-plays-role-" + UUID.randomUUID().toString();
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent, graph);

        VarAdmin resVar = Graql.var(varName).playsRole(Graql.var(typeVariable)).admin();
        atoms.add(new TypeAtom(resVar, parent));
        if (predicate != null) atoms.add(predicate);
        return atoms;
    }

    private static Set<Atomic> map(HasResourceTypeProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getVarName();
        String typeName = prop.getResourceType().getTypeName().orElse("");
        //!!!HasResourceType is a special case and it doesn't allow variables as resource types!!!

        //isa part
        VarAdmin resVar = Graql.var(varName).hasResource(typeName).admin();
        atoms.add(new TypeAtom(resVar, parent));
        return atoms;
    }

    private static Set<Atomic> map(IsaProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        Set<Atomic> atoms = new HashSet<>();
        //IsaProperty is unique within a var, so skip if this is a relation
        if (var.hasProperty(RelationProperty.class)) return atoms;

        String varName = var.getVarName();
        VarAdmin typeVar = prop.getType();
        String typeVariable = typeVar.isUserDefinedName() ?
                typeVar.getVarName() : varName + "-type-" + UUID.randomUUID().toString();
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent, graph);

        //isa part
        VarAdmin resVar = Graql.var(varName).isa(Graql.var(typeVariable)).admin();
        atoms.add(new TypeAtom(resVar, predicate, parent));
        if (predicate != null) atoms.add(predicate);
        return atoms;
    }

    private static Set<Atomic> map(HasResourceProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getVarName();
        Optional<String> type = prop.getType();
        VarAdmin valueVar = prop.getResource();
        String valueVariable = valueVar.isUserDefinedName() ?
                valueVar.getVarName() : varName + "-" + type.orElse("") + "-" + UUID.randomUUID().toString();
        Set<ValuePredicate> predicates = getValuePredicates(valueVariable, valueVar, vars, parent, graph);
        atoms.addAll(predicates);

        //add resource atom
        Var resource = Graql.var(valueVariable);
        VarAdmin resVar = type
                .map(t ->Graql.var(varName).has(t, resource))
                .orElseGet(() -> Graql.var(varName).has(resource)).admin();
        //TODO!!! currently storing single predicate only
        Predicate predicate = predicates.stream().findFirst().orElse(null);
        atoms.add(new Resource(resVar, predicate, parent));
        return atoms;
    }

    private static IdPredicate getUserDefinedIdPredicate(String typeVariable, Set<VarAdmin> vars, Query parent){
        return  vars.stream()
                .filter(v -> v.getVarName().equals(typeVariable))
                .flatMap(v -> v.hasProperty(NameProperty.class)?
                        v.getProperties(NameProperty.class).map(np -> new IdPredicate(typeVariable, np, parent)) :
                        v.getProperties(IdProperty.class).map(np -> new IdPredicate(typeVariable, np, parent)))
                .findFirst().orElse(null);
    }

    private static IdPredicate getIdPredicate(String typeVariable, VarAdmin typeVar, Set<VarAdmin> vars, Query parent, GraknGraph graph){
        IdPredicate predicate = null;
        //look for id predicate among vars
        if(typeVar.isUserDefinedName())
            predicate = getUserDefinedIdPredicate(typeVariable, vars, parent);
        else{
            NameProperty nameProp = typeVar.getProperty(NameProperty.class).orElse(null);
            if (nameProp != null)
                predicate = new IdPredicate(typeVariable, nameProp, parent);
        }
        return predicate;
    }

    private static Set<ValuePredicate> getValuePredicates(String valueVariable, VarAdmin valueVar, Set<VarAdmin> vars, Query parent, GraknGraph graph){
        Set<ValuePredicate> predicates = new HashSet<>();
        if(valueVar.isUserDefinedName()){
            vars.stream()
                    .filter(v -> v.getVarName().equals(valueVariable))
                    .flatMap(v -> v.getProperties(ValueProperty.class).map(vp -> new ValuePredicate(v.getVarName(), vp, parent)))
                    .forEach(predicates::add);
        }
        //add value atom
        else {
            valueVar.getProperties(ValueProperty.class)
                    .forEach(vp -> predicates
                            .add(new ValuePredicate(ValuePredicate.createValueVar(valueVariable, vp.getPredicate()), parent)));
        }
        return predicates;
    }

    private static String getTypeId(String typeName, GraknGraph graph){ return graph.getType(typeName).getId();}
}
