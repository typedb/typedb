/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */
package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.reasoner.ResolutionPlan;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.areDisjointTypes;

/**
 *
 * <p>
 * Atom implementation defining a resource atom corresponding to a {@link HasResourceProperty}.
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
public class ResourceAtom extends Binary{
    private final Var relationVariable;
    private final ImmutableSet<ValuePredicate> multiPredicate;

    public ResourceAtom(VarPatternAdmin pattern, Var attributeVar, Var relationVariable, @Nullable IdPredicate idPred, Set<ValuePredicate> ps, ReasonerQuery par){
        super(pattern, attributeVar, idPred, par);
        this.relationVariable = relationVariable;
        this.multiPredicate = ImmutableSet.copyOf(ps);
    }

    private ResourceAtom(ResourceAtom a) {
        super(a);
        this.relationVariable = a.getRelationVariable();
        this.multiPredicate = ImmutableSet.<ValuePredicate>builder().addAll(
                a.getMultiPredicate().stream()
                        .map(pred -> (ValuePredicate) AtomicFactory.create(pred, getParentQuery()))
                        .iterator()
        ).build();
    }

    @Override
    public String toString(){
        String multiPredicateString = getMultiPredicate().isEmpty()?
                getPredicateVariable().toString() :
                getMultiPredicate().stream().map(Predicate::getPredicate).collect(Collectors.toSet()).toString();
        return getVarName() + " has " + getSchemaConcept().getLabel() + " " +
                multiPredicateString +
                getPredicates(Predicate.class).map(Predicate::toString).collect(Collectors.joining(""))  +
                (relationVariable.isUserDefinedName()? "(" + relationVariable + ")" : "");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        ResourceAtom a2 = (ResourceAtom) obj;
        return Objects.equals(this.getTypeId(), a2.getTypeId())
                && this.getVarName().equals(a2.getVarName())
                && this.getMultiPredicate().equals(a2.getMultiPredicate());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (this.getTypeId() != null? this.getTypeId().hashCode() : 0);
        hashCode = hashCode * 37 + this.getVarName().hashCode();
        hashCode = hashCode * 37 + this.getMultiPredicate().hashCode();
        return hashCode;
    }

    private boolean hasMultiPredicateEquivalentWith(ResourceAtom atom){
        if(this.getMultiPredicate().size() != atom.getMultiPredicate().size()) return false;
        for (ValuePredicate vp : getMultiPredicate()) {
            Iterator<ValuePredicate> objIt = atom.getMultiPredicate().iterator();
            boolean predicateHasEquivalent = false;
            while (objIt.hasNext() && !predicateHasEquivalent) {
                predicateHasEquivalent = vp.isAlphaEquivalent(objIt.next());
            }
            if (!predicateHasEquivalent) return false;
        }
        return true;
    }

    @Override
    boolean hasEquivalentPredicatesWith(Binary at) {
        if (!(at instanceof ResourceAtom && super.hasEquivalentPredicatesWith(at))) return false;

        ResourceAtom atom = (ResourceAtom) at;
        if (!hasMultiPredicateEquivalentWith(atom)) return false;

        IdPredicate thisPredicate = this.getIdPredicate(getPredicateVariable());
        IdPredicate predicate = atom.getIdPredicate(atom.getPredicateVariable());
        return thisPredicate == null && predicate == null || thisPredicate != null && thisPredicate.isAlphaEquivalent(predicate);
    }

    @Override
    boolean predicateBindingsAreEquivalent(Binary at) {
        if (!(at instanceof ResourceAtom && super.predicateBindingsAreEquivalent(at))) return false;

        ResourceAtom atom = (ResourceAtom) at;
        if (!hasMultiPredicateEquivalentWith(atom)) return false;

        IdPredicate thisPredicate = this.getIdPredicate(getPredicateVariable());
        IdPredicate predicate = atom.getIdPredicate(atom.getPredicateVariable());
        return (thisPredicate == null) == (predicate == null);
    }

    @Override
    public int alphaEquivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (this.getTypeId() != null? this.getTypeId().hashCode() : 0);
        hashCode = hashCode * 37 + multiPredicateEquivalenceHashCode();
        return hashCode;
    }

    private int multiPredicateEquivalenceHashCode(){
        int hashCode = 0;
        SortedSet<Integer> hashes = new TreeSet<>();
        getMultiPredicate().forEach(atom -> hashes.add(atom.alphaEquivalenceHashCode()));
        for (Integer hash : hashes) hashCode = hashCode * 37 + hash;
        return hashCode;
    }



    @Override
    public void setParentQuery(ReasonerQuery q) {
        super.setParentQuery(q);
        multiPredicate.forEach(pred -> pred.setParentQuery(q));
    }

    public Set<ValuePredicate> getMultiPredicate() { return multiPredicate;}
    public Var getRelationVariable(){ return relationVariable;}

    @Override
    public PatternAdmin getCombinedPattern() {
        Set<VarPatternAdmin> vars = getMultiPredicate().stream()
                .map(Atomic::getPattern)
                .map(PatternAdmin::asVarPattern)
                .collect(Collectors.toSet());
        vars.add(super.getPattern().asVarPattern());
        return Patterns.conjunction(vars);
    }

    @Override
    public boolean isRuleApplicable(InferenceRule child) {
        Atom ruleAtom = child.getRuleConclusionAtom();
        if(!(ruleAtom.isResource())) return false;

        ResourceAtom childAtom = (ResourceAtom) ruleAtom;

        //check types
        TypeAtom parentTypeConstraint = this.getTypeConstraints().findFirst().orElse(null);
        TypeAtom childTypeConstraint = childAtom.getTypeConstraints().findFirst().orElse(null);

        SchemaConcept parentType = parentTypeConstraint != null? parentTypeConstraint.getSchemaConcept() : null;
        SchemaConcept childType = childTypeConstraint != null? childTypeConstraint.getSchemaConcept() : null;
        if (parentType != null && childType != null && areDisjointTypes(parentType, childType)) return false;

        //check predicates
        if (childAtom.getMultiPredicate().isEmpty() || getMultiPredicate().isEmpty()) return true;
        for (ValuePredicate childPredicate : childAtom.getMultiPredicate()) {
            Iterator<ValuePredicate> parentIt = getMultiPredicate().iterator();
            boolean predicateCompatible = false;
            while (parentIt.hasNext() && !predicateCompatible) {
                ValuePredicate parentPredicate = parentIt.next();
                predicateCompatible = parentPredicate.getPredicate().isCompatibleWith(childPredicate.getPredicate());
            }
            if (!predicateCompatible) return false;
        }
        return true;
    }

    @Override
    public Atomic copy(){ return new ResourceAtom(this);}

    @Override
    public boolean isResource(){ return true;}

    @Override
    public boolean isSelectable(){ return true;}

    @Override
    public boolean isUserDefined(){ return relationVariable.isUserDefinedName();}

    @Override
    public boolean requiresMaterialisation(){ return true;}

    @Override
    public boolean isAllowedToFormRuleHead(){
        if (getSchemaConcept() == null || getMultiPredicate().size() > 1) return false;
        if (getMultiPredicate().isEmpty()) return true;

        ValuePredicate predicate = getMultiPredicate().iterator().next();
        return predicate.getPredicate().isSpecific();
    }

    @Override
    public Set<Var> getVarNames() {
        Set<Var> varNames = super.getVarNames();
        multiPredicate.stream().flatMap(p -> p.getVarNames().stream()).forEach(varNames::add);
        if (relationVariable.isUserDefinedName()) varNames.add(relationVariable);
        return varNames;
    }

    @Override
    public Set<String> validateOntologically() {
        SchemaConcept type = getSchemaConcept();
        if (type == null) {
            return new HashSet<>();
        }

        Set<String> errors = new HashSet<>();
        if (!type.isAttributeType()){
            errors.add(ErrorMessage.VALIDATION_RULE_INVALID_RESOURCE_TYPE.getMessage(type.getLabel()));
            return errors;
        }

        SchemaConcept ownerType = getParentQuery().getVarSchemaConceptMap().get(getVarName());

        if (ownerType != null
                && ownerType.isType()
                && ownerType.asType().attributes().noneMatch(rt -> rt.equals(type.asAttributeType()))){
            errors.add(ErrorMessage.VALIDATION_RULE_RESOURCE_OWNER_CANNOT_HAVE_RESOURCE.getMessage(type.getLabel(), ownerType.getLabel()));
        }
        return errors;
    }

    private boolean isSuperNode(){
        return tx().graql().match(getCombinedPattern()).admin().stream()
                .skip(ResolutionPlan.RESOURCE_SUPERNODE_SIZE)
                .findFirst().isPresent();
    }

    @Override
    public int computePriority(Set<Var> subbedVars){
        int priority = super.computePriority(subbedVars);
        Set<ai.grakn.graql.ValuePredicate> vps = getPredicates(ValuePredicate.class).map(ValuePredicate::getPredicate).collect(Collectors.toSet());
        priority += ResolutionPlan.IS_RESOURCE_ATOM;

        if (vps.isEmpty()) {
            if (subbedVars.contains(getVarName()) || subbedVars.contains(getPredicateVariable())
                    && !isSuperNode()) {
                    priority += ResolutionPlan.SPECIFIC_VALUE_PREDICATE;
            } else{
                    priority += ResolutionPlan.VARIABLE_VALUE_PREDICATE;
            }
        } else {
            int vpsPriority = 0;
            for (ai.grakn.graql.ValuePredicate vp : vps) {
                //vp with a value
                if (vp.isSpecific() && !isSuperNode()) {
                    vpsPriority += ResolutionPlan.SPECIFIC_VALUE_PREDICATE;
                } //vp with a variable
                else if (vp.getInnerVar().isPresent()) {
                    VarPatternAdmin inner = vp.getInnerVar().orElse(null);
                    //variable mapped inside the query
                    if (subbedVars.contains(getVarName())
                        || subbedVars.contains(inner.var())
                            && !isSuperNode()) {
                        vpsPriority += ResolutionPlan.SPECIFIC_VALUE_PREDICATE;
                    } //variable equality
                    else if (vp.equalsValue().isPresent()){
                        vpsPriority += ResolutionPlan.VARIABLE_VALUE_PREDICATE;
                    } //variable inequality
                    else {
                        vpsPriority += ResolutionPlan.COMPARISON_VARIABLE_VALUE_PREDICATE;
                    }
                } else {
                    vpsPriority += ResolutionPlan.NON_SPECIFIC_VALUE_PREDICATE;
                }
            }
            //normalise
            vpsPriority = vpsPriority/vps.size();
            priority += vpsPriority;
        }

        boolean reifiesRelation =  getNeighbours(Atom.class)
                .filter(Atom::isRelation)
                .filter(at -> at.getVarName().equals(this.getVarName()))
                .findFirst().isPresent();

        priority += reifiesRelation ? ResolutionPlan.RESOURCE_REIFYING_RELATION : 0;

        return priority;
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom){
        Var attributeVariable = getPredicateVariable();
        Var relationVariable = getRelationVariable().asUserDefined();
        VarPattern newVar = getVarName()
                .has(getSchemaConcept().getLabel(), attributeVariable, relationVariable);
        return new ResourceAtom(newVar.admin(), attributeVariable, relationVariable, getTypePredicate(), getMultiPredicate(), getParentQuery());
    }

    @Override
    public Unifier getUnifier(Atom parentAtom) {
        if (!(parentAtom instanceof ResourceAtom)){
            return new UnifierImpl(ImmutableMap.of(this.getPredicateVariable(), parentAtom.getVarName()));
        }
        Unifier unifier = super.getUnifier(parentAtom);
        ResourceAtom parent = (ResourceAtom) parentAtom;

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

    @Override
    public Set<TypeAtom> getSpecificTypeConstraints() {
        return getTypeConstraints()
                .filter(t -> t.getVarName().equals(getVarName()))
                .filter(t -> Objects.nonNull(t.getSchemaConcept()))
                .collect(Collectors.toSet());
    }

}
