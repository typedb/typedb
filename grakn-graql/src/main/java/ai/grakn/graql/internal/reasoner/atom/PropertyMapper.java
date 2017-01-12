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
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.pattern.property.DataTypeProperty;
import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.pattern.property.HasResourceTypeProperty;
import ai.grakn.graql.internal.pattern.property.HasRoleProperty;
import ai.grakn.graql.internal.pattern.property.HasScopeProperty;
import ai.grakn.graql.internal.pattern.property.IdProperty;
import ai.grakn.graql.internal.pattern.property.IsAbstractProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.NameProperty;
import ai.grakn.graql.internal.pattern.property.NeqProperty;
import ai.grakn.graql.internal.pattern.property.PlaysRoleProperty;
import ai.grakn.graql.internal.pattern.property.RegexProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.pattern.property.SubProperty;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.reasoner.atom.binary.HasRole;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.atom.binary.Resource;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.atom.property.DataTypeAtom;
import ai.grakn.graql.internal.reasoner.atom.property.IsAbstractAtom;
import ai.grakn.graql.internal.reasoner.atom.property.RegexAtom;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.util.ErrorMessage;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Class providing mappings of graql properties to reasoner atoms.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class PropertyMapper {

    /**
     * map graql property to a set of reasoner atoms
     * @param prop graql property to be mapped
     * @param var variable the property is contained in
     * @param vars set of variables contained in the top level conjunction
     * @param parent query the atoms should belong to
     * @param graph of interest
     * @return set of converted atoms
     */
    public static Atomic map(VarProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph){
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
        else if (prop instanceof HasRoleProperty)
            return map((HasRoleProperty)prop, var, vars, parent, graph);
        else if (prop instanceof HasResourceTypeProperty)
            return map((HasResourceTypeProperty)prop, var, vars, parent, graph);
        else if (prop instanceof HasScopeProperty)
            return map((HasScopeProperty)prop, var, vars, parent, graph);
        else if (prop instanceof IsaProperty)
            return map((IsaProperty)prop, var, vars, parent, graph);
        else if (prop instanceof HasResourceProperty)
            return map((HasResourceProperty)prop, var, vars, parent, graph);
        else if (prop instanceof NeqProperty)
            return map((NeqProperty) prop, var, vars, parent, graph);
        else if (prop instanceof IsAbstractProperty)
            return map((IsAbstractProperty)prop, var, vars, parent, graph);
        else if (prop instanceof DataTypeProperty)
            return map((DataTypeProperty)prop, var, vars, parent, graph);
        else if (prop instanceof RegexProperty)
            return map((RegexProperty)prop, var, vars, parent, graph);
        else
            throw new IllegalArgumentException(ErrorMessage.GRAQL_PROPERTY_NOT_MAPPED.getMessage(prop.toString()));
    }

    private static Atomic map(RelationProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
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
            VarName typeVariable = typeName.isEmpty()? isaVar.getVarName() : Patterns.varName("rel-" + UUID.randomUUID().toString());
            relVar.isa(Graql.var(typeVariable));
            if (!typeName.isEmpty()) {
                VarAdmin idVar = Graql.var(typeVariable).id(graph.getType(typeName).getId()).admin();
                predicate = new IdPredicate(idVar, parent);
            }
            else
                predicate = getUserDefinedIdPredicate(typeVariable, vars, parent);
        }
        return new Relation(relVar.admin(), predicate, parent);
    }

    private static Atomic map(NeqProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        return new NotEquals(var.getVarName(), prop, parent);
    }

    private static Atomic map(IdProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        return new IdPredicate(var.getVarName(), prop, parent);
    }

    private static Atomic map(NameProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        return new IdPredicate(var.getVarName(), prop, parent);
    }

    private static Atomic map(ValueProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        return new ValuePredicate(var.getVarName(), prop, parent);
    }

    private static Atomic map(RegexProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        return new RegexAtom(var.getVarName(), prop, parent);
    }

    private static Atomic map(DataTypeProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        return new DataTypeAtom(var.getVarName(), prop, parent);
    }

    private static Atomic map(IsAbstractProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        return new IsAbstractAtom(var.getVarName(), prop, parent);
    }

    private static Atomic map(SubProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        VarName varName = var.getVarName();
        VarAdmin typeVar = prop.getSuperType();
        VarName typeVariable = typeVar.isUserDefinedName() ?
                typeVar.getVarName() : varName.map(name -> name + "-sub-" + UUID.randomUUID().toString());
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent, graph);

        VarAdmin resVar = Graql.var(varName).sub(Graql.var(typeVariable)).admin();
        return new TypeAtom(resVar, predicate, parent);
    }

    private static Atomic map(PlaysRoleProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        VarName varName = var.getVarName();
        VarAdmin typeVar = prop.getRole();
        VarName typeVariable = typeVar.isUserDefinedName() ?
                typeVar.getVarName() : varName.map(name -> name + "-plays-role-" + UUID.randomUUID().toString());
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent, graph);

        VarAdmin resVar = Graql.var(varName).playsRole(Graql.var(typeVariable)).admin();
        return new TypeAtom(resVar, predicate, parent);
    }

    private static Atomic map(HasRoleProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        VarName varName = var.getVarName();
        VarAdmin roleVar = prop.getRole();
        VarName roleVariable = roleVar.getVarName();
        IdPredicate relPredicate = getIdPredicate(varName, var, vars, parent, graph);
        IdPredicate rolePredicate = getIdPredicate(roleVariable, roleVar, vars, parent, graph);

        VarAdmin hrVar = Graql.var(varName).hasRole(Graql.var(roleVariable)).admin();
        return new HasRole(hrVar, relPredicate, rolePredicate, parent);
    }

    private static Atomic map(HasResourceTypeProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        VarName varName = var.getVarName();
        String typeName = prop.getResourceType().getTypeName().orElse("");
        //TODO NB: HasResourceType is a special case and it doesn't allow variables as resource types

        //isa part
        VarAdmin resVar = Graql.var(varName).hasResource(typeName).admin();
        return new TypeAtom(resVar, parent);
    }

    private static Atomic map(HasScopeProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        VarName varName = var.getVarName();
        VarAdmin scopeVar = prop.getScope();
        VarName scopeVariable = scopeVar.isUserDefinedName() ?
                scopeVar.getVarName() : varName.map(name -> name + "-scope-" + UUID.randomUUID().toString());
        IdPredicate predicate = getIdPredicate(scopeVariable, scopeVar, vars, parent, graph);

        //isa part
        VarAdmin scVar = Graql.var(varName).hasScope(Graql.var(scopeVariable)).admin();
        return new TypeAtom(scVar, predicate, parent);
    }

    private static Atomic map(IsaProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        //IsaProperty is unique within a var, so skip if this is a relation
        if (var.hasProperty(RelationProperty.class)) return null;

        VarName varName = var.getVarName();
        VarAdmin typeVar = prop.getType();
        VarName typeVariable = typeVar.isUserDefinedName() ?
                typeVar.getVarName() : varName.map(name -> name + "-type-" + UUID.randomUUID().toString());
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent, graph);

        //isa part
        VarAdmin resVar = Graql.var(varName).isa(Graql.var(typeVariable)).admin();
        return new TypeAtom(resVar, predicate, parent);
    }

    private static Atomic map(HasResourceProperty prop, VarAdmin var, Set<VarAdmin> vars, Query parent, GraknGraph graph) {
        VarName varName = var.getVarName();
        Optional<String> type = prop.getType();
        VarAdmin valueVar = prop.getResource();
        VarName valueVariable = valueVar.isUserDefinedName() ?
                valueVar.getVarName() : varName.map(name -> name + "-" + type.orElse("") + "-" + UUID.randomUUID().toString());
        Set<Predicate> predicates = getValuePredicates(valueVariable, valueVar, vars, parent, graph);

        //add resource atom
        Var resource = Graql.var(valueVariable);
        VarAdmin resVar = type
                .map(t ->Graql.var(varName).has(t, resource))
                .orElseGet(() -> Graql.var(varName).has(resource)).admin();
        return new Resource(resVar, predicates, parent);
    }

    private static IdPredicate getUserDefinedIdPredicate(VarName typeVariable, Set<VarAdmin> vars, Query parent){
        return  vars.stream()
                .filter(v -> v.getVarName().equals(typeVariable))
                .flatMap(v -> v.hasProperty(NameProperty.class)?
                        v.getProperties(NameProperty.class).map(np -> new IdPredicate(typeVariable, np, parent)) :
                        v.getProperties(IdProperty.class).map(np -> new IdPredicate(typeVariable, np, parent)))
                .findFirst().orElse(null);
    }

    private static IdPredicate getIdPredicate(VarName typeVariable, VarAdmin typeVar, Set<VarAdmin> vars, Query parent, GraknGraph graph){
        IdPredicate predicate = null;
        //look for id predicate among vars
        if(typeVar.isUserDefinedName())
            predicate = getUserDefinedIdPredicate(typeVariable, vars, parent);
        else{
            NameProperty nameProp = typeVar.getProperty(NameProperty.class).orElse(null);
            if (nameProp != null) predicate = new IdPredicate(typeVariable, nameProp, parent);
        }
        return predicate;
    }

    private static Set<Predicate> getValuePredicates(VarName valueVariable, VarAdmin valueVar, Set<VarAdmin> vars, Query parent, GraknGraph graph){
        Set<Predicate> predicates = new HashSet<>();
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
}
