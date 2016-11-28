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

import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.util.ErrorMessage;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.pattern.property.HasResourceTypeProperty;
import ai.grakn.graql.internal.pattern.property.IdProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.PlaysRoleProperty;
import ai.grakn.graql.internal.pattern.property.SubProperty;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.reasoner.query.Query;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class PropertyMapper {

    public static Set<Atomic> map(VarProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent){
        if(prop instanceof RelationProperty)
            return map((RelationProperty)prop, var, vars, parent);
        else if (prop instanceof IdProperty)
            return map((IdProperty)prop, var, vars, parent);
        else if(prop instanceof ValueProperty)
            return map((ValueProperty)prop, var, vars, parent);
        else if (prop instanceof SubProperty)
            return map((SubProperty)prop, var, vars, parent);
        else if (prop instanceof PlaysRoleProperty)
            return map((PlaysRoleProperty)prop, var, vars, parent);
        else if (prop instanceof HasResourceTypeProperty)
            return map((HasResourceTypeProperty)prop, var, vars, parent);
        else if (prop instanceof IsaProperty)
            return map((IsaProperty)prop, var, vars, parent);
        else if (prop instanceof HasResourceProperty)
            return map((HasResourceProperty)prop, var, vars, parent);
        else
            throw new IllegalArgumentException(ErrorMessage.GRAQL_PROPERTY_NOT_MAPPED.getMessage(prop.toString()));
    }

    //TODO all these should eventually go into atom constructors

    private static Set<Atomic> map(RelationProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent) {
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
            String type = isaVar.getId().orElse("");
            String typeVariable = type.isEmpty()? isaVar.getVarName() : "rel-" + UUID.randomUUID().toString();
            relVar.isa(Graql.var(typeVariable));
            if (!type.isEmpty()) {
                VarAdmin idVar = Graql.var(typeVariable).id(isaProp.getProperty()).admin();
                predicate = new IdPredicate(idVar, parent);
            }
            else {
                predicate = vars.stream()
                        .filter(v -> v.getName().equals(typeVariable))
                        .flatMap(v -> v.getProperties(IdProperty.class).map(vp -> new IdPredicate(vp, v, parent)))
                        .findFirst().orElse(null);
            }
        }

        atoms.add(new Relation(relVar.admin(), predicate, parent));
        if (predicate != null) atoms.add(predicate);
        return atoms;
    }

    private static Set<Atomic> map(IdProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent) {
        return Sets.newHashSet(new IdPredicate(prop, var, parent));
    }

    private static Set<Atomic> map(ValueProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent) {
        return Sets.newHashSet(new ValuePredicate(prop, var, parent));
    }

    private static Set<Atomic> map(SubProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getVarName();
        VarAdmin typeVar = prop.getSuperType();
        String typeVariable = typeVar.isUserDefinedName() ?
                typeVar.getVarName() : varName + "-sub-" + UUID.randomUUID().toString();
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent);

        VarAdmin resVar = Graql.var(varName).sub(Graql.var(typeVariable)).admin();
        atoms.add(new TypeAtom(resVar, parent));
        if (predicate != null) atoms.add(predicate);
        return atoms;
    }

    private static Set<Atomic> map(PlaysRoleProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getVarName();
        VarAdmin typeVar = prop.getRole();
        String typeVariable = typeVar.isUserDefinedName() ?
                typeVar.getVarName() : varName + "-plays-role-" + UUID.randomUUID().toString();
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent);

        VarAdmin resVar = Graql.var(varName).playsRole(Graql.var(typeVariable)).admin();
        atoms.add(new TypeAtom(resVar, parent));
        if (predicate != null) atoms.add(predicate);
        return atoms;
    }

    private static Set<Atomic> map(HasResourceTypeProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getVarName();
        String type = prop.getResourceType().getId().orElse("");
        //!!!HasResourceType is a special case and it doesn't allow variables as resource types!!!

        //isa part
        VarAdmin resVar = Graql.var(varName).hasResource(type).admin();
        atoms.add(new TypeAtom(resVar, parent));
        return atoms;
    }

    private static Set<Atomic> map(IsaProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        //IsaProperty is unique within a var, so skip if this is a relation
        if (var.hasProperty(RelationProperty.class)) return atoms;

        String varName = var.getVarName();
        VarAdmin typeVar = prop.getType();
        String typeVariable = typeVar.isUserDefinedName() ?
                typeVar.getVarName() : varName + "-type-" + UUID.randomUUID().toString();
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent);

        //isa part
        VarAdmin resVar = Graql.var(varName).isa(Graql.var(typeVariable)).admin();
        atoms.add(new TypeAtom(resVar, predicate, parent));
        if (predicate != null) atoms.add(predicate);
        return atoms;
    }

    private static Set<Atomic> map(HasResourceProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getVarName();
        Optional<String> type = prop.getType();
        VarAdmin valueVar = prop.getResource();
        String valueVariable = valueVar.isUserDefinedName() ?
                valueVar.getVarName() : varName + "-" + type.orElse("") + "-" + UUID.randomUUID().toString();
        Set<ValuePredicate> predicates = getValuePredicates(valueVariable, valueVar, vars, parent);
        atoms.addAll(predicates);

        //add resource atom
        Var resource = Graql.var(valueVariable);
        VarAdmin resVar = type
                .map(t ->Graql.var(varName).has(t, resource))
                .orElseGet(() -> Graql.var(varName).has(resource)).admin();
        //TODO!!! currently storing single predicate only
        Predicate predicate = predicates.stream().findFirst().orElse(null);
        atoms.add(new Resource(resVar, predicate, parent));
        if (predicate != null) atoms.add(predicate);
        return atoms;
    }

    private static IdPredicate getIdPredicate(String typeVariable, VarAdmin typeVar, Set<VarAdmin> vars, Query parent){
        IdPredicate predicate = null;
        //look for id predicate among vars
        if(typeVar.isUserDefinedName()){
            predicate = vars.stream()
                    .filter(v -> v.getName().equals(typeVariable))
                    .flatMap(v -> v.getProperties(IdProperty.class).map(vp -> new IdPredicate(vp, v, parent)))
                    .findFirst().orElse(null);
        }
        else{
            IdProperty idProp = typeVar.getProperty(IdProperty.class).orElse(null);
            if (idProp != null) predicate = new IdPredicate(IdPredicate.createIdVar(typeVariable, idProp.getId()), parent);
        }
        return predicate;
    }

    private static Set<ValuePredicate> getValuePredicates(String valueVariable, VarAdmin valueVar, Set<VarAdmin> vars, Query parent){
        Set<ValuePredicate> predicates = new HashSet<>();
        if(valueVar.isUserDefinedName()){
            vars.stream()
                    .filter(v -> v.getName().equals(valueVariable))
                    .flatMap(v -> v.getProperties(ValueProperty.class).map(vp -> new ValuePredicate(vp, v, parent)))
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
}
