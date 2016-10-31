package io.grakn.graql.internal.reasoner.atom;

import com.google.common.collect.Sets;
import io.grakn.graql.Graql;
import io.grakn.graql.admin.VarAdmin;
import io.grakn.graql.admin.VarProperty;
import io.grakn.graql.internal.pattern.property.HasResourceProperty;
import io.grakn.graql.internal.pattern.property.IdProperty;
import io.grakn.graql.internal.pattern.property.IsaProperty;
import io.grakn.graql.internal.pattern.property.ValueProperty;
import io.grakn.graql.internal.reasoner.query.Query;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class PropertyMapper {

    public static Set<Atomic> map(VarProperty prop, VarAdmin var, Query parent){
        if (prop instanceof ValueProperty)
            return map((ValueProperty)prop, var, parent);
        else if (prop instanceof IdProperty)
            return map((IdProperty)prop, var, parent);
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

    public static Set<Atomic> map(IsaProperty prop, VarAdmin var, Query parent) {
        Set<Atomic> atoms = new HashSet<>();
        String varName = var.getName();
        String type = prop.getType().getId().orElse("");

        VarAdmin baseVar = prop.getType();
        if (baseVar.isUserDefinedName()){
            VarAdmin resVar = Graql.var(varName).isa(Graql.var(baseVar.getName())).admin();
            atoms.add(AtomicFactory.create(resVar, parent));
        }
        else {
            VarAdmin tVar = Graql.var(varName).isa(type).admin();
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
