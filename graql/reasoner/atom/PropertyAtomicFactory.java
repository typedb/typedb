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
 *
 */

package grakn.core.graql.reasoner.atom;

import com.google.common.collect.ImmutableMap;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.atom.binary.OntologicalAtom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.atom.predicate.NeqIdPredicate;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.atom.predicate.VariableValuePredicate;
import grakn.core.graql.reasoner.atom.property.DataTypeAtom;
import grakn.core.graql.reasoner.atom.property.IsAbstractAtom;
import grakn.core.graql.reasoner.atom.property.RegexAtom;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.graql.reasoner.utils.ReasonerUtils;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.cache.QueryCache;
import grakn.core.kb.graql.reasoner.cache.RuleCache;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.property.AbstractProperty;
import graql.lang.property.DataTypeProperty;
import graql.lang.property.HasAttributeProperty;
import graql.lang.property.HasAttributeTypeProperty;
import graql.lang.property.IdProperty;
import graql.lang.property.IsaProperty;
import graql.lang.property.NeqProperty;
import graql.lang.property.PlaysProperty;
import graql.lang.property.RegexProperty;
import graql.lang.property.RelatesProperty;
import graql.lang.property.RelationProperty;
import graql.lang.property.SubProperty;
import graql.lang.property.ThenProperty;
import graql.lang.property.TypeProperty;
import graql.lang.property.ValueProperty;
import graql.lang.property.VarProperty;
import graql.lang.property.WhenProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.graql.reasoner.utils.ReasonerUtils.getLabel;
import static grakn.core.graql.reasoner.utils.ReasonerUtils.getLabelFromUserDefinedVar;
import static grakn.core.graql.reasoner.utils.ReasonerUtils.getValuePredicates;
import static grakn.core.graql.reasoner.utils.ReasonerUtils.typeFromLabel;

/**
 * Factory class for creating Atomic objects from Graql Patterns and Properties
 */
public class PropertyAtomicFactory {

    private ReasoningContext ctx;

    public PropertyAtomicFactory(ConceptManager conceptManager,
                                 RuleCache ruleCache, QueryCache queryCache, KeyspaceStatistics keyspaceStatistics) {
        this.ctx = new ReasoningContext(null, conceptManager,queryCache, ruleCache,keyspaceStatistics);
    }

    public void setReasonerQueryFactory(ReasonerQueryFactory reasonerQueryFactory) {
        this.ctx = new ReasoningContext(reasonerQueryFactory, ctx.conceptManager(), ctx.queryCache(), ctx.ruleCache(), ctx.keyspaceStatistics());
    }

    private Atomic createAtom(Variable var, VarProperty property, ReasonerQuery parent, Statement statement, Set<Statement> otherStatements) {
        if (property instanceof DataTypeProperty) {
            return dataType(var, (DataTypeProperty) property, parent);

        } else if (property instanceof HasAttributeProperty) {
            return hasAttribute(var, (HasAttributeProperty) property, parent, otherStatements);

        } else if (property instanceof HasAttributeTypeProperty) {
            return hasAttributeType(var, (HasAttributeTypeProperty) property, parent, otherStatements);

        } else if (property instanceof IdProperty) {
            return id(var, (IdProperty) property, parent);

        } else if (property instanceof AbstractProperty) {
            return isAbstract(var, parent);

        } else if (property instanceof IsaProperty) {
            return isa(var, (IsaProperty) property, parent, statement, otherStatements);

        } else if (property instanceof TypeProperty) {
            return type(var, (TypeProperty) property, parent);

        } else if (property instanceof NeqProperty) {
            return neq(var, (NeqProperty) property, parent);

        } else if (property instanceof PlaysProperty) {
            return plays(var, (PlaysProperty) property, parent, otherStatements);

        } else if (property instanceof RegexProperty) {
            return regex(var, (RegexProperty) property, parent);

        } else if (property instanceof RelatesProperty) {
            return relates(var, (RelatesProperty) property, parent, otherStatements);

        } else if (property instanceof RelationProperty) {
            return relation(var, (RelationProperty) property, parent, statement, otherStatements);

        } else if (property instanceof SubProperty) {
            return sub(var, (SubProperty) property, parent, otherStatements);

        } else if (property instanceof ValueProperty) {
            return value((ValueProperty) property, parent, statement, otherStatements);

        } else if (property instanceof ThenProperty) {
            return then();

        } else if (property instanceof WhenProperty) {
            return when();
        } else {
            throw new IllegalArgumentException("Unrecognised subclass of VarProperty");
        }
    }

    private Atomic type(Variable var, TypeProperty property, ReasonerQuery parent) {
        Label typeLabel = Label.of(property.name());
        SchemaConcept type = typeFromLabel(typeLabel, ctx.conceptManager());
        return IdPredicate.create(var.asReturnedVar(), type.id(), parent);
    }

    private Atomic when() {
        return null;
    }

    private Atomic then() {
        return null;
    }

    private Atomic regex(Variable var, RegexProperty property, ReasonerQuery parent) {
        return RegexAtom.create(var, property, parent);
    }

    private Atomic neq(Variable var, NeqProperty property, ReasonerQuery parent) {
        return NeqIdPredicate.create(var, property, parent);
    }

    private Atomic id(Variable var, IdProperty property, ReasonerQuery parent) {
        return IdPredicate.create(var, ConceptId.of(property.id()), parent);
    }


    private Atomic relation(Variable var, RelationProperty property, ReasonerQuery parent, Statement statement, Set<Statement> otherStatements) {
        //set varName as user defined if reified
        //reified if contains more properties than the RelationProperty itself and potential IsaProperty
        boolean isReified = statement.properties().stream()
                .filter(prop -> !RelationProperty.class.isInstance(prop))
                .anyMatch(prop -> !IsaProperty.class.isInstance(prop));
        Statement relVar = isReified ? new Statement(var.asReturnedVar()) : new Statement(var);

        ConceptManager conceptManager = ctx.conceptManager();
        for (RelationProperty.RolePlayer rp : property.relationPlayers()) {
            Statement rolePattern = rp.getRole().orElse(null);
            Statement rolePlayer = rp.getPlayer();
            if (rolePattern != null) {
                Variable roleVar = rolePattern.var();
                //look for indirect role definitions
                Label roleLabel = getLabelFromUserDefinedVar(roleVar, otherStatements, conceptManager);
                if (roleLabel != null) {
                    rolePattern = new Statement(roleVar).type(roleLabel.getValue());
                }
                relVar = relVar.rel(rolePattern, rolePlayer);
            } else {
                relVar = relVar.rel(rolePlayer);
            }
        }

        //isa part
        IsaProperty isaProp = statement.getProperty(IsaProperty.class).orElse(null);
        Label typeLabel = null;

        //if no isa property present generate type variable
        Variable typeVariable = isaProp != null ? isaProp.type().var() : new Variable();

        //Isa present
        if (isaProp != null) {
            Statement isaVar = isaProp.type();
            String label = isaVar.getType().orElse(null);
            if (label != null) {
                typeLabel = typeFromLabel(Label.of(label), ctx.conceptManager()).label();
            } else {
                typeVariable = isaVar.var();
                typeLabel = getLabelFromUserDefinedVar(typeVariable, otherStatements, conceptManager);
            }
        }
        relVar = isaProp != null && isaProp.isExplicit() ?
                relVar.isaX(new Statement(typeVariable.asReturnedVar())) :
                relVar.isa(new Statement(typeVariable.asReturnedVar()));
        return RelationAtom.create(relVar, typeVariable, typeLabel, parent, ctx);
    }

    private Atomic sub(Variable var, SubProperty property, ReasonerQuery parent, Set<Statement> otherStatements) {
        Label label = getLabel(property.type().var(), property.type(), otherStatements, ctx.conceptManager());
        boolean isDirect = property.isExplicit();
        if (isDirect) {
            return OntologicalAtom.create(var, property.type().var(), label, parent, OntologicalAtom.OntologicalAtomType.SubDirectAtom, ctx);
        } else {
            return OntologicalAtom.create(var, property.type().var(), label, parent, OntologicalAtom.OntologicalAtomType.SubAtom, ctx);
        }

    }

    private Atomic relates(Variable var, RelatesProperty property, ReasonerQuery parent, Set<Statement> otherStatements) {
        Label label = getLabel(property.role().var(), property.role(), otherStatements, ctx.conceptManager());
        return OntologicalAtom.create(var, property.role().var(), label, parent, OntologicalAtom.OntologicalAtomType.RelatesAtom, ctx);
    }

    private Atomic plays(Variable var, PlaysProperty property, ReasonerQuery parent, Set<Statement> otherStatements) {
        Label label = getLabel(property.role().var(), property.role(), otherStatements, ctx.conceptManager());
        return OntologicalAtom.create(var, property.role().var(), label, parent, OntologicalAtom.OntologicalAtomType.PlaysAtom, ctx);
    }

    private Atomic hasAttributeType(Variable var, HasAttributeTypeProperty property, ReasonerQuery parent, Set<Statement> otherStatements) {
        //NB: HasResourceType is a special case and it doesn't allow variables as resource types
        String label = property.attributeType().getType().orElse(null);

        Variable predicateVar = new Variable();
        SchemaConcept attributeType = ctx.conceptManager().getSchemaConcept(Label.of(label));
        Label typeLabel = attributeType != null ? attributeType.label() : null;
        return OntologicalAtom.create(var, predicateVar, typeLabel, parent, OntologicalAtom.OntologicalAtomType.HasAtom, ctx);
    }

    private Atomic hasAttribute(Variable var, HasAttributeProperty property, ReasonerQuery parent, Set<Statement> otherStatements) {
        //NB: HasAttributeProperty always has (type) label specified
        Variable varName = var.asReturnedVar();

        //NB: we always make the attribute variable explicit
        Variable attributeVariable = property.attribute().var().asReturnedVar();
        Variable relationVariable = property.relation().var();
        Variable predicateVariable = new Variable();
        Set<ValuePredicate> predicates = getValuePredicates(attributeVariable, property.attribute(), otherStatements,
                parent,this);

        IsaProperty isaProp = property.attribute().getProperties(IsaProperty.class).findFirst().orElse(null);
        Statement typeVar = isaProp != null ? isaProp.type() : null;
        Label typeLabel = typeVar != null ? getLabel(predicateVariable, typeVar, otherStatements, ctx.conceptManager()) : null;

        //add resource atom
        Statement resVar = relationVariable.isReturned() ?
                new Statement(varName).has(property.type(), new Statement(attributeVariable), new Statement(relationVariable)) :
                new Statement(varName).has(property.type(), new Statement(attributeVariable));
        return AttributeAtom.create(resVar, attributeVariable, relationVariable, predicateVariable, typeLabel, predicates, parent, ctx);
    }


    private DataTypeAtom dataType(Variable var, DataTypeProperty property, ReasonerQuery parent) {
        ImmutableMap.Builder<Graql.Token.DataType, AttributeType.DataType<?>> dataTypesBuilder = new ImmutableMap.Builder<>();
        dataTypesBuilder.put(Graql.Token.DataType.BOOLEAN, AttributeType.DataType.BOOLEAN);
        dataTypesBuilder.put(Graql.Token.DataType.DATE, AttributeType.DataType.DATE);
        dataTypesBuilder.put(Graql.Token.DataType.DOUBLE, AttributeType.DataType.DOUBLE);
        dataTypesBuilder.put(Graql.Token.DataType.LONG, AttributeType.DataType.LONG);
        dataTypesBuilder.put(Graql.Token.DataType.STRING, AttributeType.DataType.STRING);
        ImmutableMap<Graql.Token.DataType, AttributeType.DataType<?>> dataTypes = dataTypesBuilder.build();
        return DataTypeAtom.create(var, property, parent, dataTypes.get(property.dataType()));
    }

    private Atomic isAbstract(Variable var, ReasonerQuery parent) {
        return IsAbstractAtom.create(var, parent);
    }

    /**
     *
     * @param property
     * @param parent
     * @param statement
     * @param otherStatements
     * @return either a ValuePredicate or a VariableValuePredicate
     */
    public Atomic value(ValueProperty property, ReasonerQuery parent, Statement statement, Set<Statement> otherStatements) {
        ValuePredicate vp = createValuePredicate(property, statement, otherStatements, parent);
        if (vp == null) return vp;
        boolean isVariable = vp.getPredicate().innerStatement() != null;
        return isVariable? VariableValuePredicate.fromValuePredicate(vp) : vp;
    }

    public Atomic isa(Variable var, IsaProperty property, ReasonerQuery parent, Statement statement, Set<Statement> otherStatements) {
        //IsaProperty is unique within a var, so skip if this is a relation
        if (statement.hasProperty(RelationProperty.class)) return null;

        Variable typeVar = property.type().var();
        Label typeLabel = getLabel(typeVar, property.type(), otherStatements, ctx.conceptManager());
        return IsaAtom.create(var, typeVar, typeLabel, property.isExplicit(), parent, ctx);
    }

    /**
     * @param pattern conjunction of patterns to be converted to atoms
     * @param parent query the created atoms should belong to
     * @return set of atoms
     */
    public Stream<Atomic> createAtoms(Conjunction<Statement> pattern, ReasonerQuery parent) {
        //parse all atoms
        Set<Atomic> atoms = pattern.statements().stream()
                .flatMap(statement -> statement.properties().stream()
                        .map(property -> createAtom(statement.var(), property, parent, statement, pattern.statements()))
                        .filter(Objects::nonNull))
                .collect(Collectors.toSet());

        //Extract variable predicates from attributes and add them to the atom set
        //We need to treat them separately to ensure correctness - different conditions can arise with different
        //orderings of resolvable atoms - hence we need to compare at the end.
        //NB: this creates different vps because the statement context is bound to HasAttributeProperty.
        Set<Atomic> neqs = pattern.statements().stream()
                .flatMap(statement -> statement.getProperties(HasAttributeProperty.class))
                .flatMap(hp -> hp.statements().flatMap(
                        statement -> statement.getProperties(ValueProperty.class)
                                .map(property -> value(property, parent, statement, pattern.statements()))
                                .filter(Objects::nonNull))
                ).collect(Collectors.toSet());
        atoms.addAll(neqs);

        //remove duplicates
        return atoms.stream()
                .filter(at -> atoms.stream()
                        .filter(Atom.class::isInstance)
                        .map(Atom.class::cast)
                        .flatMap(Atom::getInnerPredicates)
                        .noneMatch(at::equals)
                )
                .map(Atomic::simplify);
    }

    /**
     *
     * @param property value property we are interested in
     * @param statement the value property belongs to
     * @param otherStatements other statements providing necessary context
     * @param parent query the VP should be part of
     * @return value predicate corresponding to the provided property
     */
    private ValuePredicate createValuePredicate(ValueProperty property, Statement statement, Set<Statement> otherStatements,
                                                ReasonerQuery parent) {
        HasAttributeProperty has = statement.getProperties(HasAttributeProperty.class).findFirst().orElse(null);
        Variable var = has != null? has.attribute().var() : statement.var();
        ValueProperty.Operation directOperation = property.operation();
        Variable predicateVar = directOperation.innerStatement() != null? directOperation.innerStatement().var() : null;

        boolean partOfAttribute = otherStatements.stream()
                .flatMap(s -> s.getProperties(HasAttributeProperty.class))
                .anyMatch(p -> p.attribute().var().equals(var));
        Set<ValueProperty> parentVPs = otherStatements.stream()
                .flatMap(s -> s.getProperties(ValueProperty.class))
                .filter(vp -> {
                    Statement inner = vp.operation().innerStatement();
                    if (inner == null) return false;
                    return inner.var().equals(var);
                })
                .collect(Collectors.toSet());
        //true if the VP has another VP that references it - a parent VP
        boolean hasParentVp = !parentVPs.isEmpty();
        //if (hasParentVp) return null;
        if (hasParentVp && !partOfAttribute) return null;

        //if predicate variable is bound in another atom, we always need to create a NeqPredicate
        boolean predicateVarBound = otherStatements.stream()
                .flatMap(s -> s.properties().stream())
                .filter(p -> !(p instanceof ValueProperty))
                .flatMap(VarProperty::statements)
                .map(Statement::var)
                .anyMatch(v -> v.equals(predicateVar));
        ValueProperty.Operation indirectOperation = !predicateVarBound?
                ReasonerUtils.findValuePropertyOp(predicateVar, otherStatements) : null;

        Object value = indirectOperation == null ? directOperation.value() : indirectOperation.value();
        ValueProperty.Operation operation = ValueProperty.Operation.Comparison.of(directOperation.comparator(), value);
        return ValuePredicate.create(var.asReturnedVar(), operation, parent);
    }

}
