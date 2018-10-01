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
package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.GraknTx;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.UnifierComparison;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.pattern.property.HasAttributeProperty;
import ai.grakn.graql.internal.query.answer.ConceptMapImpl;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicEquivalence;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.kb.internal.concept.AttributeImpl;
import ai.grakn.kb.internal.concept.AttributeTypeImpl;
import ai.grakn.kb.internal.concept.EntityImpl;
import ai.grakn.kb.internal.concept.RelationshipImpl;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.google.common.collect.Iterables;
import java.util.stream.Stream;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.areDisjointTypes;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.isEquivalentCollection;

/**
 *
 * <p>
 * Atom implementation defining a resource atom corresponding to a {@link HasAttributeProperty}.
 * The resource structure is the following:
 *
 * has($varName, $predicateVariable = resource variable), type($predicateVariable)
 *
 * or in graql terms:
 *
 * $varName has <type> $predicateVariable; $predicateVariable isa <type>;
 *
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@AutoValue
public abstract class ResourceAtom extends Binary{

    public abstract Var getRelationVariable();
    public abstract ImmutableSet<ValuePredicate> getMultiPredicate();

    public static ResourceAtom create(VarPattern pattern, Var attributeVar, Var relationVariable, ConceptId predicateId, Set<ValuePredicate> ps, ReasonerQuery parent) {
        return new AutoValue_ResourceAtom(pattern.admin().var(), pattern, parent, attributeVar, predicateId,  relationVariable, ImmutableSet.copyOf(ps));
    }
    private static ResourceAtom create(ResourceAtom a, ReasonerQuery parent) {
        ResourceAtom atom = create(a.getPattern(), a.getPredicateVariable(),  a.getRelationVariable(), a.getTypeId(), a.getMultiPredicate(), parent);
        atom.applicableRules = a.applicableRules;
        return atom;
    }

    @Override
    public Atomic copy(ReasonerQuery parent){ return create(this, parent);}

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() { return HasAttributeProperty.class;}

    @Override
    public RelationshipAtom toRelationshipAtom(){
        SchemaConcept type = getSchemaConcept();
        if (type == null) throw GraqlQueryException.illegalAtomConversion(this, RelationshipAtom.class);
        GraknTx tx = getParentQuery().tx();
        Label typeLabel = Schema.ImplicitType.HAS.getLabel(type.label());
        return RelationshipAtom.create(
                Graql.var()
                        .rel(Schema.ImplicitType.HAS_OWNER.getLabel(type.label()).getValue(), getVarName())
                        .rel(Schema.ImplicitType.HAS_VALUE.getLabel(type.label()).getValue(), getPredicateVariable())
                        .isa(typeLabel.getValue())
                .admin(),
                getPredicateVariable(),
                tx.getSchemaConcept(typeLabel).id(),
                getParentQuery()
        );
    }

    /**
     * NB: this is somewhat ambiguous cause from {$x has resource $r;} we can extract:
     * - $x isa ???;
     * - $r isa resource;
     * We pick the latter as the type information is available.
     *
     * @return corresponding isa atom
     */
    @Override
    public IsaAtom toIsaAtom(){
        return IsaAtom.create(getPredicateVariable(), Graql.var(), getTypeId(), false, getParentQuery());
    }

    @Override
    public String toString(){
        String multiPredicateString = getMultiPredicate().isEmpty()?
                getPredicateVariable().toString() :
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
        ResourceAtom a2 = (ResourceAtom) obj;
        return Objects.equals(this.getTypeId(), a2.getTypeId())
                && this.getVarName().equals(a2.getVarName())
                && this.multiPredicateEquivalent(a2, AtomicEquivalence.Equality);
    }

    private boolean multiPredicateEquivalent(ResourceAtom that, AtomicEquivalence equiv){
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
        if (!(at instanceof ResourceAtom && super.predicateBindingsEquivalent(at, equiv))) return false;

        ResourceAtom that = (ResourceAtom) at;
        if (!multiPredicateEquivalent(that, equiv)) return false;

        IdPredicate thisPredicate = this.getIdPredicate(this.getPredicateVariable());
        IdPredicate predicate = that.getIdPredicate(that.getPredicateVariable());

        return thisPredicate == null && predicate == null || thisPredicate != null && equiv.equivalent(thisPredicate, predicate);
    }

    @Override
    protected Pattern createCombinedPattern(){
        Set<VarPatternAdmin> vars = getMultiPredicate().stream()
                .map(Atomic::getPattern)
                .map(VarPattern::admin)
                .collect(Collectors.toSet());
        vars.add(getPattern().admin());
        return Patterns.conjunction(vars);
    }

    @Override
    public boolean isRuleApplicableViaAtom(Atom ruleAtom) {
        //findbugs complains about cast without it
        if (!(ruleAtom instanceof ResourceAtom)) return false;

        ResourceAtom childAtom = (ResourceAtom) ruleAtom;
        return childAtom.isUnifiableWith(this);
    }

    @Override
    public boolean isUnifiableWith(Atom atom) {
        //findbugs complains about cast without it
        if (!(atom instanceof ResourceAtom)) return false;

        ResourceAtom parent = (ResourceAtom) atom;
        ReasonerQueryImpl childQuery = (ReasonerQueryImpl) this.getParentQuery();

        //check type bindings compatibility
        Type childType = childQuery.getVarTypeMap().get(this.getVarName());
        Type parentType = parent.getParentQuery().getVarTypeMap().get(parent.getVarName());

        if (childType != null && parentType != null && areDisjointTypes(childType, parentType)
                || !childQuery.isTypeRoleCompatible(this.getVarName(), parentType)) return false;

        //check value predicate compatibility
        return parent.getMultiPredicate().isEmpty()
                || this.getMultiPredicate().isEmpty()
                || this.getMultiPredicate().stream().allMatch(childPredicate -> parent.getMultiPredicate().stream().anyMatch(parentPredicate -> parentPredicate.isCompatibleWith(childPredicate)));
    }

    @Override
    public boolean isResource(){ return true;}

    @Override
    public boolean isSelectable(){ return true;}

    public boolean isSpecific(){ return getMultiPredicate().stream().anyMatch(p -> p.getPredicate().isSpecific());}

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
                    .anyMatch(at -> at.getVarNames().contains(getPredicateVariable()));
            if (!predicateBound) {
                errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE.getMessage(rule.then(), rule.label()));
            }
        }

        getMultiPredicate().stream()
                .filter(p -> !p.getPredicate().isSpecific())
                .forEach( p ->
                        errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RESOURCE_WITH_NONSPECIFIC_PREDICATE.getMessage(rule.then(), rule.label()))
                );
        return errors;
    }

    @Override
    public Set<Var> getVarNames() {
        Set<Var> varNames = super.getVarNames();
        getMultiPredicate().stream().flatMap(p -> p.getVarNames().stream()).forEach(varNames::add);
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
        if (!(parentAtom instanceof ResourceAtom)) {
            if (parentAtom instanceof IsaAtom){ return this.toIsaAtom().getUnifier(parentAtom, unifierType); }
            else {
                throw GraqlQueryException.unificationAtomIncompatibility();
            }
        }

        ResourceAtom parent = (ResourceAtom) parentAtom;
        Unifier unifier = super.getUnifier(parentAtom, unifierType);

        if (unifier == null || !unifierType.attributeValueCompatibility(new HashSet<>(parent.getMultiPredicate()), new HashSet<>(this.getMultiPredicate())) ){
            return UnifierImpl.nonExistent();
        }

        //unify relation vars
        Var childRelationVarName = this.getRelationVariable();
        Var parentRelationVarName = parent.getRelationVariable();
        if (parentRelationVarName.isUserDefinedName()
                && !childRelationVarName.equals(parentRelationVarName)){
            unifier = unifier.merge(new UnifierImpl(ImmutableMap.of(childRelationVarName, parentRelationVarName)));
        }
        return unifier;
    }

    @Override
    public Stream<Predicate> getInnerPredicates(){
        return Stream.concat(super.getInnerPredicates(), getMultiPredicate().stream());
    }

    private void attachAttribute(Concept owner, Attribute attribute){
        if (owner.isEntity()){
            EntityImpl.from(owner.asEntity()).attributeInferred(attribute);
        } else if (owner.isRelationship()){
            RelationshipImpl.from(owner.asRelationship()).attributeInferred(attribute);
        } else if (owner.isAttribute()){
            AttributeImpl.from(owner.asAttribute()).attributeInferred(attribute);
        }
    }

    @Override
    public Stream<ConceptMap> materialise(){
        ConceptMap substitution = getParentQuery().getSubstitution();
        AttributeTypeImpl attributeType = AttributeTypeImpl.from(getSchemaConcept().asAttributeType());

        Concept owner = substitution.get(getVarName());
        Var resourceVariable = getPredicateVariable();

        //if the attribute already exists, only attach a new link to the owner, otherwise create a new attribute
        Attribute attribute;
        if(this.isSpecific()){
            Object value = Iterables.getOnlyElement(getMultiPredicate()).getPredicate().equalsValue().orElse(null);
            Attribute existingAttribute = attributeType.attribute(value);
            attribute = existingAttribute == null? attributeType.putAttributeInferred(value) : existingAttribute;
        } else {
            attribute = substitution.containsVar(resourceVariable)? substitution.get(resourceVariable).asAttribute() : null;
        }

        attachAttribute(owner, attribute);
        return Stream.of(substitution.merge(new ConceptMapImpl(ImmutableMap.of(resourceVariable, attribute))));
    }

    /**
     * rewrites the atom to one with relation variable
     * @param parentAtom parent atom that triggers rewrite
     * @return rewritten atom
     */
    private ResourceAtom rewriteWithRelationVariable(Atom parentAtom){
        if (parentAtom.isResource() && ((ResourceAtom) parentAtom).getRelationVariable().isUserDefinedName()) return rewriteWithRelationVariable();
        return this;
    }

    @Override
    public ResourceAtom rewriteWithRelationVariable(){
        Var attributeVariable = getPredicateVariable();
        Var relationVariable = getRelationVariable().asUserDefined();
        VarPattern newVar = getVarName().has(getSchemaConcept().label(), attributeVariable, relationVariable);
        return create(newVar.admin(), attributeVariable, relationVariable, getTypeId(), getMultiPredicate(), getParentQuery());
    }

    @Override
    public Atom rewriteWithTypeVariable() {
        return create(getPattern(), getPredicateVariable().asUserDefined(), getRelationVariable(), getTypeId(), getMultiPredicate(), getParentQuery());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom){
        return this
                .rewriteWithRelationVariable(parentAtom)
                .rewriteWithTypeVariable(parentAtom);
    }
}
