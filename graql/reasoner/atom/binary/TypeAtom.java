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

import com.google.common.collect.Sets;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.atom.predicate.Predicate;
import grakn.core.graql.reasoner.atom.task.relate.TypeAtomSemanticProcessor;
import grakn.core.graql.reasoner.atom.task.relate.SemanticProcessor;
import grakn.core.graql.reasoner.cache.SemanticDifference;
import grakn.core.graql.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.graql.reasoner.ReasonerCheckedException;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.property.IsaProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 *
 * <p>
 *
 * Atom implementation defining type atoms of the general form:
 *
 * {isa|sub|plays|relates|has}($varName, $predicateVariable), type($predicateVariable)
 *
 * Type atoms correspond to the following respective graql properties:
 * IsaProperty,
 * SubProperty,
 * PlaysProperty
 * RelatesProperty
 * HasAttributeTypeProperty
 * </p>
 *
 *
 */
public abstract class TypeAtom extends Atom {

    private final Variable predicateVariable;
    private final SemanticProcessor<TypeAtom> semanticProcessor = new TypeAtomSemanticProcessor();

    private SchemaConcept type = null;
    private IdPredicate typePredicate = null;

    TypeAtom(Variable varName, Statement pattern, ReasonerQuery reasonerQuery, @Nullable Label label,
           Variable predicateVariable, ReasoningContext ctx) {
        super(reasonerQuery, varName, pattern, label, ctx);
        this.predicateVariable = predicateVariable;
    }

    public Variable getPredicateVariable() {
        return predicateVariable;
    }

    public boolean isDirect(){
        return getPattern().getProperties(IsaProperty.class).findFirst()
                .map(IsaProperty::isExplicit).orElse(false);
    }

    @Nullable
    public IdPredicate getTypePredicate(){
        if (typePredicate == null && getTypeLabel() != null) {
            ConceptId typeId = context().conceptManager().getSchemaConcept(getTypeLabel()).id();
            typePredicate = IdPredicate.create(new Statement(getPredicateVariable()).id(typeId.getValue()), getParentQuery());
        }
        return typePredicate;
    }

    @Nullable
    @Override
    public SchemaConcept getSchemaConcept(){
        if (type == null && getTypeLabel() != null) {
            SchemaConcept concept = context().conceptManager().getSchemaConcept(getTypeLabel());
            if (concept == null) throw ReasonerCheckedException.labelNotFound(getTypeLabel());
            type = concept;
        }
        return type;
    }

    @Override
    public void checkValid() {
        IdPredicate typePredicate = getTypePredicate();
        if (typePredicate != null) typePredicate.checkValid();
    }

    @Override
    public boolean isType(){ return true;}

    @Override
    public boolean isRuleApplicableViaAtom(Atom ruleAtom) {
        if (!(ruleAtom instanceof IsaAtom)) return this.isRuleApplicableViaAtom(ruleAtom.toIsaAtom());
        return ruleAtom.isUnifiableWith(this);
    }

    @Override
    public boolean isSelectable() {
        return getTypePredicate() == null
                //disjoint atom
                || !this.getNeighbours(Atom.class).findFirst().isPresent()
                || getPotentialRules().findFirst().isPresent();
    }

    @Override
    public boolean requiresMaterialisation() {
        if (!isUserDefined()) return false;
        SchemaConcept type = getSchemaConcept();
        return isUserDefined() && type != null && type.isRelationType();
    }

    @Override
    public boolean isAlphaEquivalent(Object obj) {
        if (!isBaseEquivalent(obj)) return false;
        Atom that = (Atom) obj;
        return !this.getMultiUnifier(that, UnifierType.EXACT).equals(MultiUnifierImpl.nonExistent());
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        if (!isBaseEquivalent(obj)) return false;
        Atom that = (Atom) obj;
        return !this.getMultiUnifier(that, UnifierType.STRUCTURAL).equals(MultiUnifierImpl.nonExistent());
    }

    @Override
    public int alphaEquivalenceHashCode() {
        return Objects.hash(getTypeLabel());
    }

    @Override
    public int structuralEquivalenceHashCode() {
        return alphaEquivalenceHashCode();
    }

    boolean isBaseEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        TypeAtom that = (TypeAtom) obj;
        return (this.isUserDefined() == that.isUserDefined())
                && (this.getPredicateVariable().isReturned() == that.getPredicateVariable().isReturned())
                && this.isDirect() == that.isDirect()
                && Objects.equals(this.getTypeLabel(), that.getTypeLabel());
    }

    @Override
    public Set<Variable> getVarNames() {
        Set<Variable> vars = new HashSet<>();
        if (getVarName().isReturned()) vars.add(getVarName());
        if (getPredicateVariable().isReturned()) vars.add(getPredicateVariable());
        return vars;
    }

    @Override
    protected Pattern createCombinedPattern(){
        Set<Pattern> vars = Sets.newHashSet((Pattern) getPattern());
        IdPredicate typePredicate = getTypePredicate();
        if (typePredicate != null) vars.add(typePredicate.getPattern());
        return Graql.and(vars);
    }

    @Override
    public Stream<Predicate> getInnerPredicates(){
        IdPredicate typePredicate = getTypePredicate();
        return typePredicate != null? Stream.of(typePredicate) : Stream.empty();
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
    public SemanticDifference computeSemanticDifference(Atom childAtom, Unifier unifier) {
        return semanticProcessor.computeSemanticDifference(this, childAtom, unifier, context());
    }

    /**
     * @param u unifier to be applied
     * @return set of type atoms resulting from applying the unifier
     */
    public abstract Set<TypeAtom> unify(Unifier u);
}
