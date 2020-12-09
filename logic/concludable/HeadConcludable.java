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

package grakn.core.logic.concludable;

import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.variable.SystemReference;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.reasoner.Unification;
import grakn.core.traversal.common.Identifier;
import graql.lang.pattern.variable.Reference;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.set;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class HeadConcludable<CONSTRAINT extends Constraint, U extends HeadConcludable<CONSTRAINT, U>>
        extends Concludable<CONSTRAINT, U> {

    private HeadConcludable(CONSTRAINT constraint, Set<Variable> constraintContext) {
        super(constraint);
        copyAdditionalConstraints(constraintContext, new HashSet<>(this.constraint.variables()));
    }

    public static Set<? extends HeadConcludable> of(ThingConstraint constraint, Set<Variable> constraintContext) {
        HeadConcludable<?, ?> concludable;

        if (constraint.isRelation()) concludable = Relation.copyOf(constraint.asRelation(), constraintContext);
        else if (constraint.isHas()) concludable = Has.copyOf(constraint.asHas(), constraintContext);
        else if (constraint.isIsa()) concludable = Isa.copyOf(constraint.asIsa(), constraintContext);
        else if (constraint.isValue()) concludable = Value.copyOf(constraint.asValue(), constraintContext);
        else throw GraknException.of(ILLEGAL_STATE);

        return concludable.getGeneralisations();
    }

    abstract Set<U> getGeneralisations();

    public boolean isRelation() {
        return false;
    }

    public boolean isHas() {
        return false;
    }

    public boolean isIsa() {
        return false;
    }

    public boolean isValue() {
        return false;
    }

    public Relation asRelation() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Relation.class));
    }

    public Has asHas() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Has.class));
    }

    public Isa asIsa() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Isa.class));
    }

    public Value asValue() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Value.class));
    }


    private void copyAdditionalConstraints(Set<Variable> fromVars, Set<Variable> toVars) {
        Map<Variable, Variable> nonAnonFromVarsMap = fromVars.stream()
                .filter(variable -> !variable.identifier().reference().isAnonymous())
                .collect(Collectors.toMap(e -> e, e -> e)); // Create a map for efficient lookups
        toVars.stream().filter(variable -> !variable.identifier().reference().isAnonymous())
                .forEach(copyTo -> {
                    if (nonAnonFromVarsMap.containsKey(copyTo)) {
                        Variable copyFrom = nonAnonFromVarsMap.get(copyTo);
                        if (copyTo.isThing() && copyFrom.isThing()) {
                            copyIsaAndValues(copyFrom.asThing(), copyTo.asThing());
                        } else if (copyTo.isType() && copyFrom.isType()) {
                            copyLabelAndValueType(copyFrom.asType(), copyTo.asType());
                        } else throw GraknException.of(ILLEGAL_STATE);
                    }
                });
    }

    public static class Relation extends HeadConcludable<RelationConstraint, Relation> {

        public Relation(RelationConstraint constraint, Set<Variable> constraintContext) {
            super(constraint, constraintContext);
        }

        public static Relation copyOf(RelationConstraint constraint, Set<Variable> constraintContext) {
            return new Relation(copyConstraint(constraint), constraintContext);
        }

        @Override
        Set<Relation> getGeneralisations() {
            Set<Relation> generalisations = set(this);

            return generalisations;
        }

        @Override
        public boolean isRelation() {
            return true;
        }

        @Override
        public Relation asRelation() {
            return this;
        }
    }

    public static class Has extends HeadConcludable<HasConstraint, Has> {
        //TODO: come up with a better architecture than this. Suggestion: create an object from which a constraint can be created by applying a Variable
        private ThingVariable labelIsaOwner;
        private ThingVariable namedIsaOwner;
        private ThingVariable anonIsaOwner;
        private ThingVariable labelValOwner;
        private ThingVariable namedValOwner;
        private ThingVariable labelIsaAttr;
        private ThingVariable namedIsaAttr;
        private ThingVariable anonIsaAttr;
        private ThingVariable labelValAttr;
        private ThingVariable namedValAttr;

        private TypeVariable labelVar;
        private TypeVariable nameVar;
        private TypeVariable anonVar;
        private Object concreteVal;
        private ThingVariable variableVal;


        public Has getGeneralisationOfThis(ConjunctionConcludable.Has conjunctionConcludableHas) {
            return null;
        }

        public Has(HasConstraint constraint, Set<Variable> constraintContext) {
            super(constraint, constraintContext);
        }

        public static Has copyOf(HasConstraint constraint, Set<Variable> constraintContext) {
            return new Has(copyConstraint(constraint), constraintContext);
        }

        private static Has of(HasConstraint constraint) {
            return new Has(constraint, constraint.variables());
        }

        private Has remove_owner_value() {
            ThingVariable newOwner = removeValue(constraint.owner());
            ThingVariable attributeCopy = copyIsaAndValues(constraint.attribute());
            HasConstraint hasConstraint = newOwner.has(attributeCopy);
            return Has.of(hasConstraint);
        }

        private Has name_owner_value() {
            ThingVariable newOwner = deAnonymizeValue(constraint.owner());
            ;
            ThingVariable attributeCopy = copyIsaAndValues(constraint.attribute());
            HasConstraint hasConstraint = newOwner.has(attributeCopy);
            return Has.of(hasConstraint);
        }

        private Has name_attribute_value() {
            ThingVariable newOwner = copyIsaAndValues(constraint.owner());
            ;
            ThingVariable attributeCopy = deAnonymizeValue(constraint.attribute());
            HasConstraint hasConstraint = newOwner.has(attributeCopy);
            return Has.of(hasConstraint);
        }

        private Has name_ownerIsa() {
            ThingVariable newOwner = deAnonymizeIsa(constraint.owner());
            ThingVariable attributeCopy = copyIsaAndValues(constraint.attribute());
            HasConstraint hasConstraint = newOwner.has(attributeCopy);
            return Has.of(hasConstraint);
        }

        //TODO: etc...

        public Has getGeneralisationOf(ConjunctionConcludable.Has conjunctionConcludableHas) {
            ThingVariable concludableOwner = conjunctionConcludableHas.constraint().owner();
            ThingVariable newOwner = ThingVariable.of(concludableOwner.identifier());
            if (!concludableOwner.isa().isPresent()) {
                //TODO: need to implement hint input. put in HeadConcludable rather than Concludable.
                Set<Label> commonHints = hintIntersection(concludableOwner, constraint.owner());
                copyIsa(anonIsaOwner, newOwner);
            } else if (concludableOwner.isa().get().type().reference().isName()) {
                copyIsa(namedIsaOwner, newOwner);
            } else if (concludableOwner.isa().get().type().reference().isLabel()) {
                copyIsa(labelIsaOwner, newOwner);
            } else if (concludableOwner.isa().get().type().reference().isAnonymous()) {
                copyIsa(anonIsaOwner, newOwner);
            }

            if (!concludableOwner.value().isEmpty()) {
                ValueConstraint<?> valueConstraint = concludableOwner.value().iterator().next();
                if (!valueConstraint.isVariable()) {
                    copyValueOntoVariable(labelValOwner, newOwner);
                } else {
                    copyValueOntoVariable(namedValOwner, newOwner);
                }
            }

            ThingVariable concludableAttr = conjunctionConcludableHas.constraint().attribute();
            ThingVariable newAttr = ThingVariable.of(concludableAttr.identifier());
            if (!concludableAttr.isa().isPresent()) {
                copyIsa(anonIsaAttr, newAttr);
            } else if (concludableAttr.isa().get().type().reference().isName()) {
                copyIsa(namedIsaAttr, newAttr);
            } else if (concludableAttr.isa().get().type().reference().isLabel()) {
                copyIsa(labelIsaAttr, newAttr);
            } else if (concludableAttr.isa().get().type().reference().isAnonymous()) {
                copyIsa(anonIsaAttr, newOwner);
            }

            if (!concludableAttr.value().isEmpty()) {
                ValueConstraint<?> valueConstraint = concludableAttr.value().iterator().next();
                if (!valueConstraint.isVariable()) {
                    copyValueOntoVariable(labelValAttr, newAttr);
                } else {
                    copyValueOntoVariable(namedValAttr, newAttr);
                }
            }

            HasConstraint hasConstraint = newOwner.has(newAttr);
            return Has.of(hasConstraint);
        }

        //will want to cache...
        //actually we don't want the generlisations of the isa...
        public Has getGeneralisationOf(String ownerIsa, String ownerValue, String attrIsa, String attrValue) {
            ThingVariable newOwner = ThingVariable.of(constraint.owner().identifier());
            if (ownerIsa.equals("label")) copyIsa(labelIsaOwner, newOwner);
            else if (ownerIsa.equals("named")) copyIsa(namedIsaOwner, newOwner);
            else if (ownerIsa.equals("anon")) copyIsa(anonIsaOwner, newOwner);
            else throw GraknException.of(ILLEGAL_STATE);
            if (ownerValue.equals("label")) copyValueOntoVariable(labelValOwner, newOwner);
            else if (ownerValue.equals("named")) copyValueOntoVariable(namedValOwner, newOwner);
            else if (!ownerValue.equals("none")) throw GraknException.of(ILLEGAL_STATE);

            ThingVariable newAttr = ThingVariable.of(constraint.attribute().identifier());
            if (attrIsa.equals("label")) copyIsa(labelIsaAttr, newAttr);
            else if (attrIsa.equals("named")) copyIsa(namedIsaAttr, newAttr);
            else if (attrIsa.equals("anon")) copyIsa(anonIsaAttr, newAttr);
            else throw GraknException.of(ILLEGAL_STATE);
            if (attrValue.equals("label")) copyValueOntoVariable(labelValAttr, newAttr);
            else if (attrValue.equals("named")) copyValueOntoVariable(namedValAttr, newAttr);
            else if (!attrValue.equals("none")) throw GraknException.of(ILLEGAL_STATE);

            HasConstraint hasConstraint = newOwner.has(newAttr);

            return Has.of(hasConstraint);
        }

        @Override
        Set<Has> getGeneralisations() {
            Set<Has> generalisations = set(this);
            Set<ValueConstraint<?>> ownerValueGens = set();

            //get ownerIsaName, ownerAnonName etc...


            //name_owner_value
//            generalisations.add(new Has(deAnonymize(constraint.owner().i), constraint.variables()));
            //remove_owner_value

            //name_owner_isa
            //anon_owner_isa
            //name_attribute_value
            //remove_attribute_value
            //name_attribute_isa
            //anon_attribute_isa

            return generalisations;
        }

        @Override
        public boolean isHas() {
            return true;
        }

        @Override
        public Has asHas() {
            return this;
        }
    }

    public static class Isa extends HeadConcludable<IsaConstraint, HeadConcludable.Isa> {
        //optional?
        private TypeVariable concreteForm;
        private TypeVariable anonForm;
        private TypeVariable namedForm;
        private final Set<Label> ownerHints;
        private final Set<Label> constraintHints;


        public Isa(IsaConstraint constraint, Set<Variable> constraintContext) {
            super(constraint, constraintContext);
            this.ownerHints = constraint.typeHints();
            if (constraint.type().sub().isPresent()) this.constraintHints = constraint.type().sub().get().typeHints();
            else this.constraintHints = null;
            setForms();
        }

        private void setForms() {
            if (constraint.type().reference().isLabel()) this.concreteForm = constraint.type();
            else this.concreteForm = null;
            if (constraint.type().reference().isName()) this.namedForm = constraint.type();
            else this.namedForm = new TypeVariable(Identifier.Variable.of(new SystemReference("temp")));
            if (constraint.type().reference().isAnonymous()) this.anonForm = constraint.type();
            else this.anonForm = new TypeVariable(Identifier.Variable.of(Reference.anonymous(true), 1));
        }

        public static Isa copyOf(IsaConstraint constraint, Set<Variable> constraintContext) {
            return new Isa(copyConstraint(constraint), constraintContext);
        }

        private Isa anonymizeVariable() {
//            ThingVariable newOwner = ThingVariable.of(constraint.owner().identifier());
//            copyValuesOntoVariable(constraint.owner().value(), newOwner);
//            //TODO: get number generated based on number of anonymous vars
//            TypeVariable typeVariable = new TypeVariable(Identifier.Variable.of(Reference.anonymous(true), 1));
//
//            IsaConstraint newIsaConstraint = newOwner.isa(typeVariable, constraint.isExplicit());
//            newIsaConstraint.addHints(constraint.typeHints());
//
//            return new Isa(newIsaConstraint, constraint.variables());
            return new Isa(anonymize(constraint), constraint.variables());
        }

        private Isa deAnonymizeVariable() {
//            ThingVariable newOwner = ThingVariable.of(constraint.owner().identifier());
//            copyValuesOntoVariable(constraint.owner().value(), newOwner);
//            //TODO: ensure temp var names are temporary.
//            TypeVariable typeVariable = new TypeVariable(Identifier.Variable.of(new SystemReference("temp")));
//
//            IsaConstraint newIsaConstraint = newOwner.isa(typeVariable, constraint.isExplicit());
//            newIsaConstraint.addHints(constraint.typeHints());

            return new Isa(deAnonymize(constraint), constraint.variables());
        }

        private Isa variabliseValue() {
            ThingVariable newOwner = deAnonymizeValue(constraint.owner());


//            ValueConstraint<?> newValueConstraint = deAnonymize(constraint.owner().value().iterator().next());
//            ThingVariable newOwner = newValueConstraint.owner();
//
//            IsaConstraint newIsaConstraint = copyIsaOntoVariable(constraint, newOwner);
//            return new Isa(newIsaConstraint, constraint.variables());
            return null;
        }

        private Isa removeValue() {
//            ThingVariable newOwner = ThingVariable.of(constraint.owner().identifier());
//            IsaConstraint newIsaConstraint = copyIsaOntoVariable(constraint, newOwner);
            return new Isa(removeValue(constraint), constraint.variables());
        }

        @Override
        Set<Isa> getGeneralisations() {
            Set<Isa> generalisations = set(this);
            generalisations.add(anonymizeVariable());
            generalisations.add(deAnonymizeVariable());
            generalisations.add(variabliseValue());
            generalisations.add(removeValue());

            return generalisations;
        }

        @Override
        public boolean isIsa() {
            return true;
        }

        @Override
        public Isa asIsa() {
            return this;
        }
    }

    public static class Value extends HeadConcludable<ValueConstraint<?>, Value> {

        private Value(ValueConstraint<?> constraint, Set<Variable> constraintContext) {
            super(constraint, constraintContext);
        }

        public static Value copyOf(ValueConstraint<?> constraint, Set<Variable> constraintContext) {
            return new Value(copyConstraint(constraint), constraintContext);
        }

        Value anonymousVersion() {
            return new Value(anonymize(constraint()), constraint.variables());
        }

        private Value deAnonymise() {
//            ThingVariable newOwner = ThingVariable.of(constraint.owner().identifier());
//            ThingVariable tempVariable = new ThingVariable(Identifier.Variable.of(new SystemReference("temp")));
//            ValueConstraint.Variable valueConstraint =
//                    newOwner.valueVariable(constraint.asValue().predicate().asEquality(), tempVariable);
//            new Value(deAnonymizeValue(constraint.owner()).value()., constraint.variables());
            return new Value(deAnonymize(constraint), constraint.variables());
        }

        @Override
        Set<Value> getGeneralisations() {
            Set<Value> generalisations = set(this);
            if (!constraint.isVariable()) generalisations.add(deAnonymise());
            return generalisations;
        }



        @Override
        public boolean isValue() {
            return true;
        }

        @Override
        public Value asValue() {
            return this;
        }
    }

}

