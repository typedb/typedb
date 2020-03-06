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
package grakn.core.graql.reasoner.atom.binary;

import com.google.common.base.Equivalence;
import com.google.common.collect.ImmutableSet;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.core.AttributeValueConverter;
import grakn.core.core.Schema;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.AtomicEquivalence;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.atom.predicate.Predicate;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.atom.task.convert.AtomConverter;
import grakn.core.graql.reasoner.atom.task.convert.AttributeAtomConverter;
import grakn.core.graql.reasoner.atom.task.materialise.AttributeMaterialiser;
import grakn.core.graql.reasoner.atom.task.relate.AttributeSemanticProcessor;
import grakn.core.graql.reasoner.atom.task.relate.SemanticProcessor;
import grakn.core.graql.reasoner.atom.task.validate.AtomValidator;
import grakn.core.graql.reasoner.atom.task.validate.AttributeAtomValidator;
import grakn.core.graql.reasoner.cache.SemanticDifference;
import grakn.core.graql.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.property.HasAttributeProperty;
import graql.lang.property.ValueProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import static grakn.core.graql.reasoner.utils.ReasonerUtils.isEquivalentCollection;

/**
 *
 * <p>
 * Atom implementation defining a resource atom corresponding to a HasAttributeProperty.
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
public class AttributeAtom extends Atom{

    private final IsaAtom attributeIsa;
    private final IsaAtom ownerIsa;

    private final ImmutableSet<ValuePredicate> multiPredicate;
    private final Variable attributeVariable;
    private final Variable relationVariable;

    private final SemanticProcessor<AttributeAtom> semanticProcessor = new AttributeSemanticProcessor();
    private final AtomValidator<AttributeAtom> validator = new AttributeAtomValidator();
    private final AtomConverter<AttributeAtom> converter = new AttributeAtomConverter();

    private AttributeAtom(
            Variable varName,
            Statement pattern,
            ReasonerQuery parentQuery,
            @Nullable Label label,
            Variable predicateVariable,
            Variable relationVariable,
            Variable attributeVariable,
            ImmutableSet<ValuePredicate> multiPredicate,
            ReasoningContext ctx) {
        //super(varName, pattern, parentQuery, label, predicateVariable, ctx);
        super(parentQuery, varName, pattern, label, ctx);
        this.ownerIsa = IsaAtom.create(varName, new Variable(), label, false, parentQuery, ctx);
        this.attributeIsa = IsaAtom.create(attributeVariable, predicateVariable, label, false, parentQuery, ctx);
        this.relationVariable = relationVariable;
        this.attributeVariable = attributeVariable;
        this.multiPredicate = multiPredicate;
    }

    public Variable getRelationVariable() {
        return relationVariable;
    }
    public Variable getAttributeVariable() {
        return attributeVariable;
    }
    public ImmutableSet<ValuePredicate> getMultiPredicate() {
        return multiPredicate;
    }

    public static AttributeAtom create(Statement pattern, Variable attributeVariable,
                                       Variable relationVariable, Variable predicateVariable, @Nullable Label label,
                                       Set<ValuePredicate> ps, ReasonerQuery parent, ReasoningContext ctx) {
        return new AttributeAtom(pattern.var(), pattern, parent, label, predicateVariable, relationVariable,
                attributeVariable, ImmutableSet.copyOf(ps), ctx);
    }

    private static AttributeAtom create(AttributeAtom a, ReasonerQuery parent) {
        return create(a.getPattern(), a.getAttributeVariable(), a.getRelationVariable(), a.getPredicateVariable(),
                a.getTypeLabel(), a.getMultiPredicate(), parent, a.context());
    }

    private AttributeAtom convertValues(){
        SchemaConcept type = getSchemaConcept();
        AttributeType<Object> attributeType = type.isAttributeType()? type.asAttributeType() : null;
        if (attributeType == null || Schema.MetaSchema.isMetaLabel(attributeType.label())) return this;

        AttributeType.DataType<Object> dataType = attributeType.dataType();
        Set<ValuePredicate> newMultiPredicate = this.getMultiPredicate().stream().map(vp -> {
            Object value = vp.getPredicate().value();
            if (value == null) return vp;
            Object convertedValue;
            try {
                convertedValue = AttributeValueConverter.of(dataType).convert(value);
            } catch (ClassCastException e){
                throw GraqlSemanticException.incompatibleAttributeValue(dataType, value);
            }
            ValueProperty.Operation operation = ValueProperty.Operation.Comparison.of(vp.getPredicate().comparator(), convertedValue);
            return ValuePredicate.create(vp.getVarName(), operation, getParentQuery());
        }).collect(Collectors.toSet());
        return create(getPattern(), getAttributeVariable(), getRelationVariable(), getPredicateVariable(),
                getTypeLabel(), newMultiPredicate, getParentQuery(), context());
    }

    public IsaAtom ownerIsa(){
        return ownerIsa;
    }

    public IsaAtom attributeIsa(){
        return attributeIsa;
    }

    @Override
    public Atomic copy(ReasonerQuery parent){ return create(this, parent);}

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() { return HasAttributeProperty.class;}

    @Override
    public Atomic simplify() {
        return this.convertValues();
    }

    @Override
    public AttributeAtom toAttributeAtom(){ return converter.toAttributeAtom(this, context());}

    @Override
    public RelationAtom toRelationAtom(){
        return converter.toRelationAtom(this, context());
    }

    @Override
    public IsaAtom toIsaAtom() {
        return converter.toIsaAtom(this, context());
    }

    @Override
    public String toString(){
        String multiPredicateString = getMultiPredicate().isEmpty()?
                "" :
                getMultiPredicate().stream().map(Predicate::getPredicate).collect(Collectors.toSet()).toString();
        return getVarName() + " has " + getTypeLabel() + " " +
                getAttributeVariable() + " " +
                multiPredicateString +
                (getRelationVariable().isReturned()? "(" + getRelationVariable() + ")" : "") +
                getPredicates(IdPredicate.class).map(Predicate::toString).collect(Collectors.joining(""));
    }

    @Override
    public SchemaConcept getSchemaConcept() {
        return attributeIsa.getSchemaConcept();
    }

    @Override
    public Variable getPredicateVariable() {
        return attributeIsa.getPredicateVariable();
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        AttributeAtom a2 = (AttributeAtom) obj;
        return Objects.equals(this.getTypeLabel(), a2.getTypeLabel())
                && this.getVarName().equals(a2.getVarName())
                && this.multiPredicateEqual(a2);
    }

    @Override
    public boolean isAlphaEquivalent(Object obj) {
        if (!isBaseEquivalent(obj, AtomicEquivalence.AlphaEquivalence)) return false;
        Atom that = (Atom) obj;
        return !this.getMultiUnifier(that, UnifierType.EXACT).equals(MultiUnifierImpl.nonExistent());
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        if (!isBaseEquivalent(obj, AtomicEquivalence.StructuralEquivalence)) return false;
        Atom that = (Atom) obj;
        return !this.getMultiUnifier(that, UnifierType.STRUCTURAL).equals(MultiUnifierImpl.nonExistent());
    }

    private boolean isBaseEquivalent(Object obj, Equivalence<Atomic> equivalence){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        AttributeAtom that = (AttributeAtom) obj;
        return equivalence.equivalent(this.attributeIsa(), that.attributeIsa())
                && equivalence.equivalent(this.ownerIsa(), that.ownerIsa())
                && this.getRelationVariable().isReturned() == that.getRelationVariable().isReturned();
    }

    private boolean multiPredicateEqual(AttributeAtom that){
        return isEquivalentCollection(this.getMultiPredicate(), that.getMultiPredicate(), AtomicEquivalence.Equality);
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
        hashCode = hashCode * 37 + (this.getTypeLabel() != null? this.getTypeLabel().hashCode() : 0);
        hashCode = hashCode * 37 + AtomicEquivalence.equivalenceHash(this.getMultiPredicate(), AtomicEquivalence.AlphaEquivalence);
        return hashCode;
    }

    @Override
    public int structuralEquivalenceHashCode() {
        return alphaEquivalenceHashCode();
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
    public boolean isAttribute(){ return true;}

    @Override
    public boolean isSelectable(){ return true;}

    public boolean isValueEquality(){ return getMultiPredicate().stream().anyMatch(p -> p.getPredicate().isValueEquality());}

    @Override
    public Set<Variable> getVarNames() {
        Set<Variable> varNames = super.getVarNames();
        varNames.add(getAttributeVariable());
        if (getRelationVariable().isReturned()) varNames.add(getRelationVariable());
        getMultiPredicate().forEach(p -> varNames.addAll(p.getVarNames()));
        return varNames;
    }

    @Override
    public boolean hasUniqueAnswer(){
        ConceptMap sub = getParentQuery().getSubstitution();
        if (sub.vars().containsAll(getVarNames())) return true;

        SchemaConcept type = getSchemaConcept();
        if (type == null) return false;

        boolean isBottomType = type.subs().allMatch(t -> t.equals(type));
        boolean relationVarMapped = !getRelationVariable().isReturned() || sub.containsVar(getRelationVariable());
        return isBottomType
                && isValueEquality() && sub.containsVar(getVarName())
                && relationVarMapped;
    }

    @Override
    public boolean requiresMaterialisation(){ return true;}

    @Override
    public void checkValid(){ validator.checkValid(this, context()); }

    @Override
    public Set<String> validateAsRuleHead(Rule rule){
        return validator.validateAsRuleHead(this, rule, context());
    }

    @Override
    public Set<String> validateAsRuleBody(Label ruleLabel) {
        return validator.validateAsRuleBody(this, ruleLabel, context());
    }

    @Override
    public Stream<ConceptMap> materialise() {
        return new AttributeMaterialiser().materialise(this, context());
    }

    @Override
    public Unifier getUnifier(Atom parentAtom, UnifierType unifierType) {
        return semanticProcessor.getUnifier(this, parentAtom, unifierType, context());
    }

    @Override
    public MultiUnifier getMultiUnifier(Atom parentAtom, UnifierType unifierType) {
        return semanticProcessor.getMultiUnifier(this, parentAtom, unifierType, context());
    }

    @Override
    public SemanticDifference computeSemanticDifference(Atom child, Unifier unifier) {
        return semanticProcessor.computeSemanticDifference(this, child, unifier, context());
    }

    @Override
    public Stream<Predicate> getInnerPredicates(){
        return Stream.concat(attributeIsa.getInnerPredicates(), getMultiPredicate().stream());
    }

    /**
     * rewrites the atom to one with relation variable
     * @param parentAtom parent atom that triggers rewrite
     * @return rewritten atom
     */
    private AttributeAtom rewriteWithRelationVariable(Atom parentAtom){
        if (parentAtom.isAttribute() && ((AttributeAtom) parentAtom).getRelationVariable().isReturned()) return rewriteWithRelationVariable();
        return this;
    }

    @Override
    public AttributeAtom rewriteWithRelationVariable(){
        Variable attributeVariable = getAttributeVariable();
        Variable relationVariable = getRelationVariable().asReturnedVar();
        Statement newVar = new Statement(getVarName())
                .has(getTypeLabel().getValue(), new Statement(attributeVariable), new Statement(relationVariable));
        return create(newVar, attributeVariable, relationVariable, getPredicateVariable(), getTypeLabel(), getMultiPredicate(), getParentQuery(), context());
    }

    @Override
    public Atom rewriteWithTypeVariable() {
        return create(getPattern(), getAttributeVariable(), getRelationVariable(), getPredicateVariable().asReturnedVar(), getTypeLabel(), getMultiPredicate(), getParentQuery(), context());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom){
        return this
                .rewriteWithRelationVariable(parentAtom)
                .rewriteWithTypeVariable(parentAtom);
    }
}
