/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.reasoner.concludable;

import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.constraint.type.SubConstraint;
import grakn.core.pattern.variable.SystemReference;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.traversal.common.Identifier;
import graql.lang.pattern.variable.Reference;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class Concludable<C extends Constraint, T extends Concludable<C, T>> {

    final C constraint;

    Concludable(C constraint) {
        this.constraint = constraint;
    }

    public C constraint() {
        return constraint;
    }

    //TODO: ask why everything here is static
    static RelationConstraint copyConstraint(RelationConstraint relationConstraint) {
        ThingVariable ownerCopy = copyIsaAndValues(relationConstraint.owner());
        List<RelationConstraint.RolePlayer> rolePlayersCopy = copyRolePlayers(relationConstraint.players());
        return new RelationConstraint(ownerCopy, rolePlayersCopy);
    }

    static List<RelationConstraint.RolePlayer> copyRolePlayers(List<RelationConstraint.RolePlayer> players) {
        return players.stream().map(rolePlayer -> {
            TypeVariable roleTypeCopy = rolePlayer.roleType().isPresent() ? copyVariableWithLabelAndValueType(rolePlayer.roleType().get()) : null;
            ThingVariable playerCopy = copyIsaAndValues(rolePlayer.player());
            RelationConstraint.RolePlayer rolePlayerCopy = new RelationConstraint.RolePlayer(roleTypeCopy, playerCopy);
            rolePlayerCopy.addRoleTypeHints(rolePlayer.roleTypeHints());
            return rolePlayerCopy;
        }).collect(Collectors.toList());
    }

    static HasConstraint copyConstraint(HasConstraint hasConstraint) {
        ThingVariable ownerCopy = copyIsaAndValues(hasConstraint.owner());
        ThingVariable attributeCopy = copyIsaAndValues(hasConstraint.attribute());
        return ownerCopy.has(attributeCopy);
    }

    static IsaConstraint copyConstraint(IsaConstraint isa) {
        ThingVariable newOwner = ThingVariable.of(isa.owner().identifier());
        copyValuesOntoVariable(isa.owner().value(), newOwner);
        return copyIsaOntoVariable(isa, newOwner);
    }

    static ValueConstraint<?> copyConstraint(ValueConstraint<?> value) {
        //NOTE: isa can never exist on a Value Concludable (or else it would be a Isa Concludable).
        ThingVariable newOwner = ThingVariable.of(value.owner().identifier());
        Set<ValueConstraint<?>> otherValues = value.owner().value().stream().filter(value1 -> value != value1)
                .collect(Collectors.toSet());
        copyValuesOntoVariable(otherValues, newOwner);
        return copyValueOntoVariable(value, newOwner);
    }

    static IsaConstraint copyIsaOntoVariable(IsaConstraint toCopy, ThingVariable variableToConstrain) {
        TypeVariable typeCopy = copyVariableWithLabelAndValueType(toCopy.type());
        IsaConstraint newIsa = variableToConstrain.isa(typeCopy, toCopy.isExplicit());
        newIsa.addHints(toCopy.typeHints());
        return newIsa;
    }

    static void copyValuesOntoVariable(Set<ValueConstraint<?>> toCopy, ThingVariable newOwner) {
        toCopy.forEach(valueConstraint -> copyValueOntoVariable(valueConstraint, newOwner));
    }

    static ValueConstraint<?> copyValueOntoVariable(ValueConstraint<?> toCopy, ThingVariable toConstrain) {
        ValueConstraint<?> value;
        if (toCopy.isLong())
            value = toConstrain.valueLong(toCopy.asLong().predicate().asEquality(), toCopy.asLong().value());
        else if (toCopy.isDouble())
            value = toConstrain.valueDouble(toCopy.asDouble().predicate().asEquality(), toCopy.asDouble().value());
        else if (toCopy.isBoolean())
            value = toConstrain.valueBoolean(toCopy.asBoolean().predicate().asEquality(), toCopy.asBoolean().value());
        else if (toCopy.isString())
            value = toConstrain.valueString(toCopy.asString().predicate(), toCopy.asString().value());
        else if (toCopy.isDateTime())
            value = toConstrain.valueDateTime(toCopy.asDateTime().predicate().asEquality(), toCopy.asDateTime().value());
        else if (toCopy.isVariable()) {
            ThingVariable copyOfVar = copyIsaAndValues(toCopy.asVariable().value());
            value = toConstrain.valueVariable(toCopy.asValue().predicate().asEquality(), copyOfVar);
        }
        else throw GraknException.of(ILLEGAL_STATE);
        return value;
    }

    static void copyValueOntoVariable(ThingVariable oldOwner, ThingVariable newOwner) {
        copyValueOntoVariable(oldOwner.value().iterator().next(), newOwner);
    }

    static ThingVariable copyIsa(ThingVariable copyFrom) {
        ThingVariable copy = ThingVariable.of(copyFrom.identifier());
        if (copyFrom.isa().isPresent()) copyIsaOntoVariable(copyFrom.isa().get(), copy);
        return copy;
    }

    static ThingVariable copyIsaAndValues(ThingVariable copyFrom) {
        ThingVariable copy = ThingVariable.of(copyFrom.identifier());
        copyIsaAndValues(copyFrom, copy);
        return copy;
    }

    static void copyIsaAndValues(ThingVariable oldOwner, ThingVariable newOwner) {
        if (oldOwner.isa().isPresent()) copyIsaOntoVariable(oldOwner.isa().get(), newOwner);
        copyValuesOntoVariable(oldOwner.value(), newOwner);
    }

    static void copyIsa(ThingVariable oldOwner, ThingVariable newOwner) {
        if (oldOwner.isa().isPresent()) copyIsaOntoVariable(oldOwner.isa().get(), newOwner);
    }

    static void copyLabelAndValueType(TypeVariable copyFrom, TypeVariable copyTo) {
        if (copyFrom.label().isPresent()) copyTo.label(Label.of(copyFrom.label().get().label()));
        if (copyFrom.sub().isPresent()){
            SubConstraint subCopy = copyFrom.sub().get();
            copyTo.sub(subCopy.type(), subCopy.isExplicit());
            copyTo.sub().get().addHints(subCopy.typeHints());
        }
        if (copyFrom.valueType().isPresent()) copyTo.valueType(copyFrom.valueType().get().valueType());
    }

    static TypeVariable copyVariableWithLabelAndValueType(TypeVariable copyFrom) {
        TypeVariable copy = TypeVariable.of(copyFrom.identifier());
        copyLabelAndValueType(copyFrom, copy);
        return copy;
    }



    //====________+++++++++++++++++++++++++_________========\\



    static ThingVariable deAnonymizeValue(ThingVariable thingVariable) {
        ValueConstraint<?> valueConstraint = deAnonymize(thingVariable.value().iterator().next());
        ThingVariable newOwner = valueConstraint.owner();
        return newOwner;
    }

    static ThingVariable removeValue(ThingVariable thingVariable) {
        return copyIsa(thingVariable);
    }

    static ThingVariable deAnonymizeIsa(ThingVariable thingVariable) {
        assert thingVariable.isa().isPresent();
        IsaConstraint isaConstraint = deAnonymize(thingVariable.isa().get());
        ThingVariable newOwner = isaConstraint.owner();
//        copyValuesOntoVariable(thingVariable.value(), newOwner);
        return newOwner;
    }

    static IsaConstraint deAnonymize(IsaConstraint isaConstraint) {
        ThingVariable newOwner = ThingVariable.of(isaConstraint.owner().identifier());
        copyValuesOntoVariable(isaConstraint.owner().value(), newOwner);
        TypeVariable typeVariable = new TypeVariable(Identifier.Variable.of(new SystemReference("temp")));

        IsaConstraint newIsaConstraint = newOwner.isa(typeVariable, isaConstraint.isExplicit());
        newIsaConstraint.addHints(isaConstraint.typeHints());
        return newIsaConstraint;
    }

    static IsaConstraint anonymize(IsaConstraint isaConstraint) {
        ThingVariable newOwner = ThingVariable.of(isaConstraint.owner().identifier());
        copyValuesOntoVariable(isaConstraint.owner().value(), newOwner);
        TypeVariable typeVariable = new TypeVariable(Identifier.Variable.of(Reference.anonymous(true), 1));

        IsaConstraint newIsaConstraint = newOwner.isa(typeVariable, isaConstraint.isExplicit());
        newIsaConstraint.addHints(isaConstraint.typeHints());
        return newIsaConstraint;
    }

    static ValueConstraint<?> deAnonymize(ValueConstraint<?> valueConstraint) {
//        ThingVariable newOwner = ThingVariable.of(valueConstraint.owner().identifier());
        ThingVariable newOwner =  copyIsa(valueConstraint.owner());
        ThingVariable tempVariable = new ThingVariable(Identifier.Variable.of(new SystemReference("temp")));
        return newOwner.valueVariable(valueConstraint.asValue().predicate().asEquality(), tempVariable);
    }

    static ValueConstraint<?> anonymize(ValueConstraint<?> valueConstraint) {
        ThingVariable newOwner =  copyIsa(valueConstraint.owner());
        ThingVariable tempVariable = new ThingVariable(Identifier.Variable.of(Reference.anonymous(false), 1));
        return newOwner.valueVariable(valueConstraint.asValue().predicate().asEquality(), tempVariable);
    }

    static IsaConstraint removeValue(IsaConstraint isaConstraint) {
        ThingVariable newOwner = ThingVariable.of(isaConstraint.owner().identifier());
        return copyIsaOntoVariable(isaConstraint, newOwner);
    }

    static boolean hasHints(Variable variable) {
        if (variable.isThing()) {
            return variable.asThing().isa().isPresent() && !variable.asThing().isa().get().typeHints().isEmpty();
        } else if (variable.isType()) {
            return variable.asType().sub().isPresent() && !variable.asType().sub().get().typeHints().isEmpty();
        } else {
            throw GraknException.of(ILLEGAL_STATE);
        }
    }

    static Set<Label> hintIntersection(ThingVariable first, ThingVariable second) {
        //TODO:
        if (hasHints(first) && hasHints(second)) {
//            Set<Label>
        } else if (hasHints(first)) {

        } else if (hasHints(second)) {

        } else {
            return Collections.emptySet();
        }
        return null;
    }

    static boolean hintsIntersect(Variable first, Variable second) {
        //TODO:
        return true;
    }

    static boolean hintsIntersect(RelationConstraint.RolePlayer first, RelationConstraint.RolePlayer second) {
        //TODO:
        return true;
    }

}
