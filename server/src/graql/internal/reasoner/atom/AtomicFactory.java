/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.internal.reasoner.atom;

import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.reasoner.atom.binary.HasAtom;
import grakn.core.graql.internal.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.internal.reasoner.atom.binary.PlaysAtom;
import grakn.core.graql.internal.reasoner.atom.binary.RelatesAtom;
import grakn.core.graql.internal.reasoner.atom.binary.RelationshipAtom;
import grakn.core.graql.internal.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.internal.reasoner.atom.binary.SubAtom;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.internal.reasoner.atom.predicate.NeqPredicate;
import grakn.core.graql.internal.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.internal.reasoner.atom.property.DataTypeAtom;
import grakn.core.graql.internal.reasoner.atom.property.IsAbstractAtom;
import grakn.core.graql.internal.reasoner.atom.property.RegexAtom;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.DataTypeProperty;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.HasAttributeTypeProperty;
import grakn.core.graql.query.pattern.property.IdProperty;
import grakn.core.graql.query.pattern.property.IsAbstractProperty;
import grakn.core.graql.query.pattern.property.IsaExplicitProperty;
import grakn.core.graql.query.pattern.property.IsaProperty;
import grakn.core.graql.query.pattern.property.LabelProperty;
import grakn.core.graql.query.pattern.property.NeqProperty;
import grakn.core.graql.query.pattern.property.PlaysProperty;
import grakn.core.graql.query.pattern.property.RegexProperty;
import grakn.core.graql.query.pattern.property.RelatesProperty;
import grakn.core.graql.query.pattern.property.RelationProperty;
import grakn.core.graql.query.pattern.property.SubProperty;
import grakn.core.graql.query.pattern.property.ThenProperty;
import grakn.core.graql.query.pattern.property.ValueProperty;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.graql.query.pattern.property.WhenProperty;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;
import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.getUserDefinedIdPredicate;
import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.getValuePredicates;
import static grakn.core.graql.query.pattern.Pattern.var;

/**
 * Factory class for creating {@link Atomic} objects.
 */
@SuppressWarnings("Duplicates")
public class AtomicFactory {

    /**
     * @param pattern conjunction of patterns to be converted to atoms
     * @param parent query the created atoms should belong to
     * @return set of atoms
     */
    public static Stream<Atomic> createAtoms(Conjunction<Statement> pattern, ReasonerQuery parent) {
        Set<Atomic> atoms = pattern.statements().stream()
                .flatMap(var -> var.properties().stream()
                        .map(vp -> mapToAtom(vp, var, pattern.statements(), parent))
                        .filter(Objects::nonNull))
                .collect(Collectors.toSet());

        return atoms.stream()
                .filter(at -> atoms.stream()
                        .filter(Atom.class::isInstance)
                        .map(Atom.class::cast)
                        .flatMap(Atom::getInnerPredicates)
                        .noneMatch(at::equals)
                );
    }

    /**
     * maps a var property to a reasoner atom
     *
     * @param varProperty   the var property to be converted to a reasoner atom
     * @param var           Statement this property belongs to
     * @param vars          Statements constituting the pattern this property belongs to
     * @param parent        reasoner query this atom should belong to
     * @return created atom
     */
    private static Atomic mapToAtom(VarProperty varProperty, Statement var, Set<Statement> vars, ReasonerQuery parent) {
        if (varProperty instanceof DataTypeProperty) {
            return dataTypePropertyToAtom((DataTypeProperty) varProperty, var, parent);

        } else if (varProperty instanceof HasAttributeProperty) {
            return hasAttributePropertyToAtom((HasAttributeProperty) varProperty, var, vars, parent);

        } else if (varProperty instanceof HasAttributeTypeProperty) {
            return hasAttributeTypePropertyToAtom((HasAttributeTypeProperty) varProperty, var, parent);

        } else if (varProperty instanceof IdProperty) {
            return idPropertyToAtom((IdProperty) varProperty, var, parent);

        } else if (varProperty instanceof IsAbstractProperty) {
            return isAbstractPropertyToAtom(var, parent);

        } else if (varProperty instanceof IsaProperty) {
            return isaPropertyToAtom((IsaProperty) varProperty, var, vars, parent);

        } else if (varProperty instanceof LabelProperty) {
            return labelPropertyToAtom((LabelProperty) varProperty, var, parent);

        } else if (varProperty instanceof NeqProperty) {
            return neqPropertyToAtom((NeqProperty) varProperty, var, parent);

        } else if (varProperty instanceof PlaysProperty) {
            return playsPropertyToAtom((PlaysProperty) varProperty, var, vars, parent);

        } else if (varProperty instanceof RegexProperty) {
            return regexPropertyToAtom((RegexProperty) varProperty, var, parent);

        } else if (varProperty instanceof RelatesProperty) {
            return relatesPropertyToAtom((RelatesProperty) varProperty, var, vars, parent);

        } else if (varProperty instanceof RelationProperty) {
            return relationshipPropertyToAtom((RelationProperty) varProperty, var, vars, parent);

        } else if (varProperty instanceof ThenProperty || varProperty instanceof WhenProperty) {
            return null; // TODO: Why does this return null?

        } else if (varProperty instanceof SubProperty) {
            return subPropertyToAtom((SubProperty) varProperty, var, vars, parent);

        } else if (varProperty instanceof ValueProperty) {
            return valuePropertyToAtom((ValueProperty) varProperty, var, parent);

        }

        else {
            throw new IllegalArgumentException("Unrecognised subclass of " + VarProperty.class.getName());
        }
    }

    private static IsaAtom isaPropertyToAtom(IsaProperty property, Statement var, Set<Statement> vars, ReasonerQuery parent) {
        //IsaProperty is unique within a var, so skip if this is a relation
        if (var.hasProperty(RelationProperty.class)) return null;

        Variable varName = var.var().asUserDefined();
        Statement typePattern = property.type();
        Variable typeVariable = typePattern.var();

        IdPredicate predicate = getIdPredicate(typeVariable, typePattern, vars, parent);
        ConceptId predicateId = predicate != null ? predicate.getPredicate() : null;

        //isa part
        Statement isaVar;

        if (property instanceof IsaExplicitProperty) {
            isaVar = varName.isaExplicit(typeVariable);
        } else {
            isaVar = varName.isa(typeVariable);
        }

        return IsaAtom.create(varName, typeVariable, isaVar, predicateId, parent);
    }

    private static SubAtom subPropertyToAtom(SubProperty property, Statement var, Set<Statement> vars, ReasonerQuery parent) {
        Variable varName = var.var().asUserDefined();
        Statement typeVar = property.type();
        Variable typeVariable = typeVar.var();
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent);
        ConceptId predicateId = predicate != null ? predicate.getPredicate() : null;
        return SubAtom.create(varName, typeVariable, predicateId, parent);
    }

    private static DataTypeAtom dataTypePropertyToAtom(DataTypeProperty property, Statement var, ReasonerQuery parent) {
        return DataTypeAtom.create(var.var(), property, parent);
    }

    private static AttributeAtom hasAttributePropertyToAtom(HasAttributeProperty property, Statement var, Set<Statement> vars, ReasonerQuery parent) {
        //NB: HasAttributeProperty always has (type) label specified
        Variable varName = var.var().asUserDefined();

        Variable relationVariable = property.relationship().var();
        Variable attributeVariable = property.attribute().var().asUserDefined();
        Variable predicateVariable = Pattern.var();
        Set<ValuePredicate> predicates = getValuePredicates(attributeVariable, property.attribute(), vars, parent);

        IsaProperty isaProp = property.attribute().getProperties(IsaProperty.class).findFirst().orElse(null);
        Statement typeVar = isaProp != null ? isaProp.type() : null;
        IdPredicate predicate = typeVar != null ? getIdPredicate(predicateVariable, typeVar, vars, parent) : null;
        ConceptId predicateId = predicate != null ? predicate.getPredicate() : null;

        //add resource atom
        Statement resVar = relationVariable.isUserDefinedName() ?
                varName.has(property.type(), attributeVariable, relationVariable) :
                varName.has(property.type(), attributeVariable);
        AttributeAtom atom = AttributeAtom.create(resVar, attributeVariable, relationVariable, predicateVariable, predicateId, predicates, parent);
        return atom;
    }

    private static HasAtom hasAttributeTypePropertyToAtom(HasAttributeTypeProperty property, Statement var, ReasonerQuery parent) {
        //NB: HasResourceType is a special case and it doesn't allow variables as resource types
        Variable varName = var.var().asUserDefined();
        Label label = property.attributeType().getTypeLabel().orElse(null);

        Variable predicateVar = var();
        SchemaConcept schemaConcept = parent.tx().getSchemaConcept(label);
        ConceptId predicateId = schemaConcept != null ? schemaConcept.id() : null;
        //isa part
        Statement resVar = varName.has(Pattern.label(label));
        return HasAtom.create(resVar, predicateVar, predicateId, parent);
    }

    private static IdPredicate idPropertyToAtom(IdProperty property, Statement var, ReasonerQuery parent) {
        return IdPredicate.create(var.var(), property.id(), parent);
    }

    private static IsAbstractAtom isAbstractPropertyToAtom(Statement var, ReasonerQuery parent) {
        return IsAbstractAtom.create(var.var(), parent);
    }

    private static IdPredicate labelPropertyToAtom(LabelProperty property, Statement var, ReasonerQuery parent) {
        SchemaConcept schemaConcept = parent.tx().getSchemaConcept(property.label());
        if (schemaConcept == null) throw GraqlQueryException.labelNotFound(property.label());
        return IdPredicate.create(var.var().asUserDefined(), property.label(), parent);
    }

    private static NeqPredicate neqPropertyToAtom(NeqProperty property, Statement var, ReasonerQuery parent) {
        return NeqPredicate.create(var.var(), property, parent);
    }

    private static PlaysAtom playsPropertyToAtom(PlaysProperty property, Statement var, Set<Statement> vars, ReasonerQuery parent) {
        Variable varName = var.var().asUserDefined();
        Statement typeVar = property.role();
        Variable typeVariable = typeVar.var();
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent);
        ConceptId predicateId = predicate == null ? null : predicate.getPredicate();
        return PlaysAtom.create(varName, typeVariable, predicateId, parent);
    }

    private static RegexAtom regexPropertyToAtom(RegexProperty property, Statement var, ReasonerQuery parent) {
        return RegexAtom.create(var.var(), property, parent);
    }

    private static RelatesAtom relatesPropertyToAtom(RelatesProperty property, Statement var, Set<Statement> vars, ReasonerQuery parent) {
        Variable varName = var.var().asUserDefined();
        Statement roleVar = property.role();
        Variable roleVariable = roleVar.var();
        IdPredicate predicate = getIdPredicate(roleVariable, roleVar, vars, parent);
        ConceptId predicateId = predicate != null ? predicate.getPredicate() : null;
        return RelatesAtom.create(varName, roleVariable, predicateId, parent);
    }

    private static RelationshipAtom relationshipPropertyToAtom(RelationProperty property, Statement var, Set<Statement> vars, ReasonerQuery parent) {
        //set varName as user defined if reified
        //reified if contains more properties than the RelationshipProperty itself and potential IsaProperty
        boolean isReified = var.properties().stream()
                .filter(prop -> !RelationProperty.class.isInstance(prop))
                .anyMatch(prop -> !IsaProperty.class.isInstance(prop));
        Statement relVar = isReified ? var.var().asUserDefined() : var.var();

        for (RelationProperty.RolePlayer rp : property.relationPlayers()) {
            Statement rolePattern = rp.getRole().orElse(null);
            Statement rolePlayer = rp.getPlayer();
            if (rolePattern != null) {
                Variable roleVar = rolePattern.var();
                //look for indirect role definitions
                IdPredicate roleId = getUserDefinedIdPredicate(roleVar, vars, parent);
                if (roleId != null) {
                    Concept concept = parent.tx().getConcept(roleId.getPredicate());
                    if (concept != null) {
                        if (concept.isRole()) {
                            Label roleLabel = concept.asSchemaConcept().label();
                            rolePattern = roleVar.label(roleLabel);
                        } else {
                            throw GraqlQueryException.nonRoleIdAssignedToRoleVariable(var);
                        }
                    }
                }
                relVar = relVar.rel(rolePattern, rolePlayer);
            } else relVar = relVar.rel(rolePlayer);
        }

        //isa part
        IsaProperty isaProp = var.getProperty(IsaProperty.class).orElse(null);
        IdPredicate predicate = null;

        //if no isa property present generate type variable
        Variable typeVariable = isaProp != null ? isaProp.type().var() : Pattern.var();

        //Isa present
        if (isaProp != null) {
            Statement isaVar = isaProp.type();
            Label label = isaVar.getTypeLabel().orElse(null);
            if (label != null) {
                predicate = IdPredicate.create(typeVariable, label, parent);
            } else {
                typeVariable = isaVar.var();
                predicate = getUserDefinedIdPredicate(typeVariable, vars, parent);
            }
        }
        ConceptId predicateId = predicate != null ? predicate.getPredicate() : null;
        relVar = isaProp instanceof IsaExplicitProperty ?
                relVar.isaExplicit(typeVariable.asUserDefined()) :
                relVar.isa(typeVariable.asUserDefined());
        return RelationshipAtom.create(relVar, typeVariable, predicateId, parent);
    }

    private static ValuePredicate valuePropertyToAtom(ValueProperty property, Statement var, ReasonerQuery parent) {
        return ValuePredicate.create(var.var(), property.predicate(), parent);
    }
}
