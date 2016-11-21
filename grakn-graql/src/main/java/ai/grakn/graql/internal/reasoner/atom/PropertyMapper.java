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

import ai.grakn.graql.Var;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import com.google.common.collect.Sets;
import ai.grakn.graql.Graql;
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
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class PropertyMapper {

    public static Set<Atomic> map(VarProperty prop, VarAdmin var, Query parent){
        if(prop instanceof RelationProperty)
            return map((RelationProperty)prop, var, parent);
        else if (prop instanceof IdProperty)
            return map((IdProperty)prop, var, parent);
        else if(prop instanceof ValueProperty)
            return map((ValueProperty)prop, var, parent);
        else if (prop instanceof SubProperty)
            return map((SubProperty)prop, var, parent);
        else if (prop instanceof PlaysRoleProperty)
            return map((PlaysRoleProperty)prop, var, parent);
        else if (prop instanceof HasResourceTypeProperty)
            return map((HasResourceTypeProperty)prop, var, parent);
        else if (prop instanceof IsaProperty)
            return map((IsaProperty)prop, var, parent);
        else if (prop instanceof HasResourceProperty)
            return map((HasResourceProperty)prop, var, parent);
        else
            return Sets.newHashSet(AtomicFactory.create(var, parent));
    }

    //TODO all these should eventually go into atom constructors

    private static Set<Atomic> map(RelationProperty prop, VarAdmin var, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        Set<RelationPlayer> relationPlayers = prop.getRelationPlayers().collect(Collectors.toSet());
        Var relVar = var.isUserDefinedName()? Graql.var(var.getName()) : Graql.var();

        IsaProperty isaProp = var.getProperty(IsaProperty.class).orElse(null);
        if (isaProp != null) relVar.isa(isaProp.getProperty());
        relationPlayers.forEach(rp -> {
            VarAdmin role = rp.getRoleType().orElse(null);
            VarAdmin rolePlayer = rp.getRolePlayer();
            if (role != null) relVar.rel(role, rolePlayer);
            else relVar.rel(rolePlayer);
        });
        atoms.add(new Relation(relVar.admin(), parent));
        return atoms;
    }

    private static Set<Atomic> map(IdProperty prop, VarAdmin var, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getName();
        String type = prop.getId();
        if (!type.isEmpty()) {
            VarAdmin idVar = Graql.var(varName).id(type).admin();
            atoms.add(AtomicFactory.create(idVar, parent));
        }
        return atoms;
    }

    private static Set<Atomic> map(ValueProperty prop, VarAdmin var, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        VarAdmin valueVar = Graql.var(var.getName()).value(prop.getPredicate()).admin();
        atoms.add(AtomicFactory.create(valueVar, parent));
        return atoms;
    }

    private static Set<Atomic> map(SubProperty prop, VarAdmin var, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getName();
        VarAdmin baseVar = prop.getSuperType();
        String valueVariable = baseVar.isUserDefinedName() ?
                baseVar.getName() : varName + "-sub-" + UUID.randomUUID().toString();

        //id part
        if (!baseVar.isUserDefinedName()) {
            String type = prop.getSuperType().getId().orElse("");
            VarAdmin tVar = Graql.var(valueVariable).id(type).admin();
            atoms.add(AtomicFactory.create(tVar, parent));
        }

        //isa part
        VarAdmin resVar = Graql.var(varName).sub(Graql.var(valueVariable)).admin();
        atoms.add(AtomicFactory.create(resVar, parent));
        return atoms;
    }

    private static Set<Atomic> map(PlaysRoleProperty prop, VarAdmin var, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getName();
        VarAdmin baseVar = prop.getRole();
        String valueVariable = baseVar.isUserDefinedName() ?
                baseVar.getName() : varName + "-plays-role-" + UUID.randomUUID().toString();
        //id part
        if (!baseVar.isUserDefinedName()) {
            String type = prop.getRole().getId().orElse("");
            VarAdmin tVar = Graql.var(valueVariable).id(type).admin();
            atoms.add(AtomicFactory.create(tVar, parent));
        }
        //isa part
        VarAdmin resVar = Graql.var(varName).playsRole(Graql.var(valueVariable)).admin();
        atoms.add(AtomicFactory.create(resVar, parent));
        return atoms;
    }

    private static Set<Atomic> map(HasResourceTypeProperty prop, VarAdmin var, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getName();
        String type = prop.getResourceType().getId().orElse("");
        //!!!HasResourceType is a special case and it doesn't allow variables as resource types!!!

        //isa part
        VarAdmin resVar = Graql.var(varName).hasResource(type).admin();
        atoms.add(AtomicFactory.create(resVar, parent));
        return atoms;
    }

    private static Set<Atomic> map(IsaProperty prop, VarAdmin var, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        //IsaProperty is unique within a var, so skip if this is a relation
        if (var.hasProperty(RelationProperty.class)) return atoms;

        String varName = var.getName();
        VarAdmin baseVar = prop.getType();
        String valueVariable = baseVar.isUserDefinedName() ?
                baseVar.getName() : varName + "-type-" + UUID.randomUUID().toString();

        //id part
        if (!baseVar.isUserDefinedName()) {
            String type = prop.getType().getId().orElse("");
            VarAdmin tVar = Graql.var(valueVariable).id(type).admin();
            atoms.add(AtomicFactory.create(tVar, parent));
        }
        //isa part
        VarAdmin resVar = Graql.var(varName).isa(Graql.var(valueVariable)).admin();
        atoms.add(AtomicFactory.create(resVar, parent));
        return atoms;
    }

    private static Set<Atomic> map(HasResourceProperty prop, VarAdmin var, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getName();
        String type = prop.getType();

        VarAdmin baseVar = prop.getResource();
        String valueVariable = baseVar.isUserDefinedName() ?
                baseVar.getName() : varName + "-" + type + "-" + UUID.randomUUID().toString();

        //add resource atom
        VarAdmin resVar = Graql.var(varName).has(type, Graql.var(valueVariable)).admin();
        atoms.add(AtomicFactory.create(resVar, parent));

        //add value atom
        baseVar.getProperties(ValueProperty.class).forEach(valProp -> {
            VarAdmin resourceValueVar = Graql.var(valueVariable).value(valProp.getPredicate()).admin();
            atoms.add(AtomicFactory.create(resourceValueVar, parent));
        });
        return atoms;
    }
}
