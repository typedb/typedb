package io.mindmaps.graql.internal.reasoner.atom;

import com.google.common.collect.Sets;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.admin.VarProperty;
import io.mindmaps.graql.internal.pattern.property.HasResourceProperty;
import io.mindmaps.graql.internal.pattern.property.HasResourceTypeProperty;
import io.mindmaps.graql.internal.pattern.property.IdProperty;
import io.mindmaps.graql.internal.pattern.property.IsaProperty;
import io.mindmaps.graql.internal.pattern.property.PlaysRoleProperty;
import io.mindmaps.graql.internal.pattern.property.SubProperty;
import io.mindmaps.graql.internal.pattern.property.ValueProperty;
import io.mindmaps.graql.internal.reasoner.query.Query;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class PropertyMapper {

    public static Set<Atomic> map(VarProperty prop, VarAdmin var, Query parent){
        if (prop instanceof ValueProperty)
            return map((ValueProperty)prop, var, parent);
        else if (prop instanceof IdProperty)
            return map((IdProperty)prop, var, parent);
        else if (prop instanceof SubProperty)
            return map((SubProperty)prop, var, parent);
        else if (prop instanceof PlaysRoleProperty)
            return map((PlaysRoleProperty)prop, var, parent);
        else if (prop instanceof IsaProperty)
            return map((IsaProperty)prop, var, parent);
        else if (prop instanceof HasResourceProperty)
            return map((HasResourceProperty)prop, var, parent);
        else
            return Sets.newHashSet(AtomicFactory.create(var, parent));
    }

    public static Set<Atomic> map(ValueProperty prop, VarAdmin var, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        VarAdmin valueVar = Graql.var(var.getName()).value(prop.getPredicate()).admin();
        atoms.add(AtomicFactory.create(valueVar, parent));
        return atoms;
    }

    public static Set<Atomic> map(IdProperty prop, VarAdmin var, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getName();
        String type = prop.getId();
        if (!type.isEmpty()) {
            VarAdmin idVar = Graql.var(varName).id(type).admin();
            atoms.add(AtomicFactory.create(idVar, parent));
        }
        return atoms;
    }

    public static Set<Atomic> map(SubProperty prop, VarAdmin var, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getName();
        String type = prop.getSuperType().getId().orElse("");
        VarAdmin baseVar = prop.getSuperType();

        //isa part
        String valueVariable = baseVar.isUserDefinedName() ?
                baseVar.getName() : varName + "-sub-" + UUID.randomUUID().toString();
        VarAdmin resVar = Graql.var(varName).sub(Graql.var(valueVariable)).admin();
        atoms.add(AtomicFactory.create(resVar, parent));

        //id part
        if (!baseVar.isUserDefinedName()) {
            VarAdmin tVar = Graql.var(valueVariable).id(type).admin();
            atoms.add(AtomicFactory.create(tVar, parent));
        }
        return atoms;
    }

    public static Set<Atomic> map(PlaysRoleProperty prop, VarAdmin var, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getName();
        String type = prop.getRole().getId().orElse("");
        VarAdmin baseVar = prop.getRole();

        //isa part
        String valueVariable = baseVar.isUserDefinedName() ?
                baseVar.getName() : varName + "-plays-role-" + UUID.randomUUID().toString();
        VarAdmin resVar = Graql.var(varName).playsRole(Graql.var(valueVariable)).admin();
        atoms.add(AtomicFactory.create(resVar, parent));

        //id part
        if (!baseVar.isUserDefinedName()) {
            VarAdmin tVar = Graql.var(valueVariable).id(type).admin();
            atoms.add(AtomicFactory.create(tVar, parent));
        }
        return atoms;
    }

    public static Set<Atomic> map(HasResourceTypeProperty prop, VarAdmin var, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getName();
        String type = prop.getResourceType().getId().orElse("");
        VarAdmin baseVar = prop.getResourceType();

        //isa part
        String valueVariable = baseVar.isUserDefinedName() ?
                baseVar.getName() : varName + "-has-resource-" + UUID.randomUUID().toString();
        VarAdmin resVar = Graql.var(varName).hasResource(Graql.var(valueVariable)).admin();
        atoms.add(AtomicFactory.create(resVar, parent));

        //id part
        if (!baseVar.isUserDefinedName()) {
            VarAdmin tVar = Graql.var(valueVariable).id(type).admin();
            atoms.add(AtomicFactory.create(tVar, parent));
        }
        return atoms;
    }

    public static Set<Atomic> map(IsaProperty prop, VarAdmin var, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getName();
        String type = prop.getType().getId().orElse("");

        VarAdmin baseVar = prop.getType();

        //isa part
        String valueVariable = baseVar.isUserDefinedName() ?
                baseVar.getName() : varName + "-type-" + UUID.randomUUID().toString();
        VarAdmin resVar = Graql.var(varName).isa(Graql.var(valueVariable)).admin();
        atoms.add(AtomicFactory.create(resVar, parent));

        //id part
        if (!baseVar.isUserDefinedName()) {
            VarAdmin tVar = Graql.var(valueVariable).id(type).admin();
            atoms.add(AtomicFactory.create(tVar, parent));
        }

        return atoms;
    }

    public static Set<Atomic> map(HasResourceProperty prop, VarAdmin var, Query parent) {
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
        baseVar.getValuePredicates().forEach(pred -> {
            VarAdmin resourceValueVar = Graql.var(valueVariable).value(pred).admin();
            atoms.add(AtomicFactory.create(resourceValueVar, parent));
        });
        return atoms;
    }
}
