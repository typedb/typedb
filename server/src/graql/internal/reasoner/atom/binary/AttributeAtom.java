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
package grakn.core.graql.internal.reasoner.atom.binary;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.internal.reasoner.atom.Atomic;
import grakn.core.graql.internal.reasoner.query.ReasonerQuery;
import grakn.core.graql.internal.reasoner.unifier.Unifier;
import grakn.core.graql.internal.reasoner.unifier.UnifierComparison;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.Rule;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.atom.AtomicEquivalence;
import grakn.core.graql.internal.reasoner.atom.predicate.Predicate;
import grakn.core.graql.internal.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.internal.reasoner.cache.SemanticDifference;
import grakn.core.graql.internal.reasoner.cache.VariableDefinition;
import grakn.core.graql.internal.reasoner.unifier.UnifierImpl;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.graql.query.pattern.statement.Variable;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.server.Transaction;
import grakn.core.server.kb.concept.AttributeImpl;
import grakn.core.server.kb.concept.AttributeTypeImpl;
import grakn.core.server.kb.concept.EntityImpl;
import grakn.core.server.kb.concept.RelationshipImpl;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.isEquivalentCollection;

/**
 *
 * <p>
 * Atom implementation defining a resource atom corresponding to a {@link HasAttributeProperty}.
 * The resource structure is the following:
 *
 * has($varName, $attributeVariable), type($attributeVariable)
 *
 * or in graql terms:
 *
 * $varName has <type> $attributeVariable;
 * $attributeVariable isa $predicateVariable; [$predicateVariable/<type id>]
 *
 * </p>
 *
 *
 */
@AutoValue
public abstract class AttributeAtom extends Binary{

    public abstract Variable getRelationVariable();
    public abstract Variable getAttributeVariable();
    public abstract ImmutableSet<ValuePredicate> getMultiPredicate();

    public static AttributeAtom create(Statement pattern, Variable attributeVariable, Variable relationVariable, Variable predicateVariable, ConceptId predicateId, Set<ValuePredicate> ps, ReasonerQuery parent) {
        return new AutoValue_AttributeAtom(pattern.var(), pattern, parent, predicateVariable, predicateId, relationVariable, attributeVariable, ImmutableSet.copyOf(ps));
    }

    private static AttributeAtom create(AttributeAtom a, ReasonerQuery parent) {
        return create(a.getPattern(), a.getAttributeVariable(), a.getRelationVariable(), a.getPredicateVariable(), a.getTypeId(), a.getMultiPredicate(), parent);
    }

    @Override
    public Atomic copy(ReasonerQuery parent){ return create(this, parent);}

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() { return HasAttributeProperty.class;}

    @Override
    public RelationshipAtom toRelationshipAtom(){
        SchemaConcept type = getSchemaConcept();
        if (type == null) throw GraqlQueryException.illegalAtomConversion(this, RelationshipAtom.class);
        Transaction tx = getParentQuery().tx();
        Label typeLabel = Schema.ImplicitType.HAS.getLabel(type.label());
        return RelationshipAtom.create(
                Graql.var()
                        .rel(Schema.ImplicitType.HAS_OWNER.getLabel(type.label()).getValue(), new Statement(getVarName()))
                        .rel(Schema.ImplicitType.HAS_VALUE.getLabel(type.label()).getValue(), new Statement(getAttributeVariable()))
                        .isa(typeLabel.getValue()),
                getPredicateVariable(),
                tx.getSchemaConcept(typeLabel).id(),
                getParentQuery()
        );
    }

    /**
     * NB: this is somewhat ambiguous cause from {$x has resource $r;} we can extract:
     * - $r isa owner-type;
     * - $x isa attribute-type;
     * We pick the latter as the type information is available.
     *
     * @return corresponding isa atom
     */
    @Override
    public IsaAtom toIsaAtom(){
        return IsaAtom.create(getAttributeVariable(), new Variable(), getTypeId(), false, getParentQuery());
    }

    @Override
    public String toString(){
        String multiPredicateString = getMultiPredicate().isEmpty()?
                getAttributeVariable().toString() :
                getMultiPredicate().stream().map(Predicate::getPredicate).collect(Collectors.toSet()).toString();
        return getVarName() + " has " + getSchemaConcept().label() + " " +
                multiPredicateString +
                getPredicates(Predicate.class).map(Predicate::toString).collect(Collectors.joining(""))  +
                (getRelationVariable().isUserDefinedName()? "(" + getRelationVariable() + ")" : "");
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        AttributeAtom a2 = (AttributeAtom) obj;
        return Objects.equals(this.getTypeId(), a2.getTypeId())
                && this.getVarName().equals(a2.getVarName())
                && this.multiPredicateEquivalent(a2, AtomicEquivalence.Equality);
    }

    private boolean multiPredicateEquivalent(AttributeAtom that, AtomicEquivalence equiv){
        return isEquivalentCollection(this.getMultiPredicate(), that.getMultiPredicate(), equiv);
    }

    @Override
    public final int hashCode() {
        int hashCode = this.alphaEquivalenceHashCode();
        hashCode = hashCode * 37 + this.getVarName().hashCode();
        return hashCode;
    }

    @Override
    public int alphaEquivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (this.getTypeId() != null? this.getTypeId().hashCode() : 0);
        hashCode = hashCode * 37 + AtomicEquivalence.equivalenceHash(this.getMultiPredicate(), AtomicEquivalence.AlphaEquivalence);
        return hashCode;
    }

    @Override
    boolean predicateBindingsEquivalent(Binary at, AtomicEquivalence equiv) {
        if (!(at instanceof AttributeAtom && super.predicateBindingsEquivalent(at, equiv))) return false;
        AttributeAtom that = (AttributeAtom) at;
        return predicateBindingsEquivalent(this.getAttributeVariable(), that.getAttributeVariable(), that, equiv);
    }

    @Override
    protected Pattern createCombinedPattern(){
        Set<Statement> vars = getMultiPredicate().stream()
                .map(Atomic::getPattern)
                .collect(Collectors.toSet());
        vars.add(getPattern());
        return Graql.and(vars);
    }

    @Override
    public boolean isRuleApplicableViaAtom(Atom ruleAtom) {
        //findbugs complains about cast without it
        if (!(ruleAtom instanceof AttributeAtom)) return false;

        AttributeAtom childAtom = (AttributeAtom) ruleAtom;
        return childAtom.isUnifiableWith(this);
    }

    @Override
    public boolean isResource(){ return true;}

    @Override
    public boolean isSelectable(){ return true;}

    public boolean isValueEquality(){ return getMultiPredicate().stream().anyMatch(p -> p.getPredicate().isValueEquality());}

    @Override
    public boolean requiresMaterialisation(){ return true;}

    @Override
    public Set<String> validateAsRuleHead(Rule rule){
        Set<String> errors = super.validateAsRuleHead(rule);
        if (getSchemaConcept() == null || getMultiPredicate().size() > 1){
            errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RESOURCE_WITH_AMBIGUOUS_PREDICATES.getMessage(rule.then(), rule.label()));
        }
        if (getMultiPredicate().isEmpty()){
            boolean predicateBound = getParentQuery().getAtoms(Atom.class)
                    .filter(at -> !at.equals(this))
                    .anyMatch(at -> at.getVarNames().contains(getAttributeVariable()));
            if (!predicateBound) {
                errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE.getMessage(rule.then(), rule.label()));
            }
        }

        getMultiPredicate().stream()
                .filter(p -> !p.getPredicate().isValueEquality())
                .forEach( p ->
                        errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RESOURCE_WITH_NONSPECIFIC_PREDICATE.getMessage(rule.then(), rule.label()))
                );
        return errors;
    }

    @Override
    public Set<Variable> getVarNames() {
        Set<Variable> varNames = super.getVarNames();
        varNames.add(getAttributeVariable());
        if (getRelationVariable().isUserDefinedName()) varNames.add(getRelationVariable());
        return varNames;
    }

    @Override
    public Set<String> validateOntologically() {
        SchemaConcept type = getSchemaConcept();
        Set<String> errors = new HashSet<>();
        if (type == null) return errors;

        if (!type.isAttributeType()){
            errors.add(ErrorMessage.VALIDATION_RULE_INVALID_ATTRIBUTE_TYPE.getMessage(type.label()));
            return errors;
        }

        Type ownerType = getParentQuery().getVarTypeMap().get(getVarName());

        if (ownerType != null
                && ownerType.attributes().noneMatch(rt -> rt.equals(type.asAttributeType()))){
            errors.add(ErrorMessage.VALIDATION_RULE_ATTRIBUTE_OWNER_CANNOT_HAVE_ATTRIBUTE.getMessage(type.label(), ownerType.label()));
        }
        return errors;
    }

    @Override
    public Unifier getUnifier(Atom parentAtom, UnifierComparison unifierType) {
        if (!(parentAtom instanceof AttributeAtom)) {
            if (parentAtom instanceof IsaAtom){ return this.toIsaAtom().getUnifier(parentAtom, unifierType); }
            else {
                throw GraqlQueryException.unificationAtomIncompatibility();
            }
        }

        AttributeAtom parent = (AttributeAtom) parentAtom;
        Unifier unifier = super.getUnifier(parentAtom, unifierType);
        if (unifier == null) return UnifierImpl.nonExistent();

        //unify attribute vars
        Variable childAttributeVarName = this.getAttributeVariable();
        Variable parentAttributeVarName = parent.getAttributeVariable();
        if (parentAttributeVarName.isUserDefinedName()){
            unifier = unifier.merge(new UnifierImpl(ImmutableMap.of(childAttributeVarName, parentAttributeVarName)));
        }

        //unify relation vars
        Variable childRelationVarName = this.getRelationVariable();
        Variable parentRelationVarName = parent.getRelationVariable();
        if (parentRelationVarName.isUserDefinedName()){
            unifier = unifier.merge(new UnifierImpl(ImmutableMap.of(childRelationVarName, parentRelationVarName)));
        }

        return isPredicateCompatible(parentAtom, unifier, unifierType)?
                unifier : UnifierImpl.nonExistent();
    }

    @Override
    public Stream<Predicate> getInnerPredicates(){
        return Stream.concat(super.getInnerPredicates(), getMultiPredicate().stream());
    }

    private void attachAttribute(Concept owner, Attribute attribute){
        //check if link exists
        if (owner.asThing().attributes(attribute.type()).noneMatch(a -> a.equals(attribute))) {
            if (owner.isEntity()) {
                EntityImpl.from(owner.asEntity()).attributeInferred(attribute);
            } else if (owner.isRelationship()) {
                RelationshipImpl.from(owner.asRelation()).attributeInferred(attribute);
            } else if (owner.isAttribute()) {
                AttributeImpl.from(owner.asAttribute()).attributeInferred(attribute);
            }
        }
    }

    @Override
    public SemanticDifference semanticDifference(Atom p, Unifier unifier) {
        SemanticDifference baseDiff = super.semanticDifference(p, unifier);
        if (!p.isResource()) return baseDiff;
        AttributeAtom parentAtom = (AttributeAtom) p;
        Set<VariableDefinition> diff = new HashSet<>();
        Unifier unifierInverse = unifier.inverse();
        Variable childVar = getAttributeVariable();
        Set<ValuePredicate> predicates = new HashSet<>(getMultiPredicate());
        parentAtom.getMultiPredicate().stream()
                .flatMap(vp -> vp.unify(unifierInverse).stream())
                .forEach(predicates::remove);
        diff.add(new VariableDefinition(childVar, null, null, new HashSet<>(), predicates));
        return baseDiff.merge(new SemanticDifference(diff));
    }

    @Override
    public Stream<ConceptMap> materialise(){
        ConceptMap substitution = getParentQuery().getSubstitution();
        AttributeTypeImpl attributeType = AttributeTypeImpl.from(getSchemaConcept().asAttributeType());

        Concept owner = substitution.get(getVarName());
        Variable resourceVariable = getAttributeVariable();

        //if the attribute already exists, only attach a new link to the owner, otherwise create a new attribute
        Attribute attribute;
        if(this.isValueEquality()){
            Object value = Iterables.getOnlyElement(getMultiPredicate()).getPredicate().value();
            Attribute existingAttribute = attributeType.attribute(value);
            attribute = existingAttribute == null? attributeType.putAttributeInferred(value) : existingAttribute;
        } else {
            attribute = substitution.containsVar(resourceVariable)? substitution.get(resourceVariable).asAttribute() : null;
        }

        if (attribute != null) {
            attachAttribute(owner, attribute);
            return Stream.of(substitution.merge(new ConceptMap(ImmutableMap.of(resourceVariable, attribute))));
        }
        return Stream.of(new ConceptMap());
    }

    /**
     * rewrites the atom to one with relation variable
     * @param parentAtom parent atom that triggers rewrite
     * @return rewritten atom
     */
    private AttributeAtom rewriteWithRelationVariable(Atom parentAtom){
        if (parentAtom.isResource() && ((AttributeAtom) parentAtom).getRelationVariable().isUserDefinedName()) return rewriteWithRelationVariable();
        return this;
    }

    @Override
    public AttributeAtom rewriteWithRelationVariable(){
        Variable attributeVariable = getAttributeVariable();
        Variable relationVariable = getRelationVariable().asUserDefined();
        Statement newVar = new Statement(getVarName())
                .has(getSchemaConcept().label().getValue(), new Statement(attributeVariable), new Statement(relationVariable));
        return create(newVar, attributeVariable, relationVariable, getPredicateVariable(), getTypeId(), getMultiPredicate(), getParentQuery());
    }

    @Override
    public Atom rewriteWithTypeVariable() {
        return create(getPattern(), getAttributeVariable(), getRelationVariable(), getPredicateVariable().asUserDefined(), getTypeId(), getMultiPredicate(), getParentQuery());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom){
        return this
                .rewriteWithRelationVariable(parentAtom)
                .rewriteWithTypeVariable(parentAtom);
    }
}
