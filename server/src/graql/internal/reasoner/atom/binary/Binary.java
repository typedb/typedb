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

import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.Var;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.admin.UnifierComparison;
import grakn.core.graql.query.pattern.property.IsaExplicitProperty;
import grakn.core.graql.internal.reasoner.unifier.UnifierImpl;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.atom.AtomicEquivalence;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;

import grakn.core.graql.internal.reasoner.atom.predicate.NeqPredicate;
import grakn.core.graql.internal.reasoner.atom.predicate.Predicate;
import grakn.core.graql.internal.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.internal.reasoner.unifier.UnifierType;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 *
 * <p>
 * Implementation for binary atoms with single id typePredicate for a schema concept. Binary atoms take the form:
 *
 * <>($varName, $predicateVariable), type($predicateVariable)
 *
 * </p>
 *
 *
 */
public abstract class Binary extends Atom {

    public abstract Var getPredicateVariable();
    @Nullable @Override public abstract ConceptId getTypeId();

    private SchemaConcept type = null;
    private IdPredicate typePredicate = null;

    @Nullable
    public IdPredicate getTypePredicate(){
        if (typePredicate == null && getTypeId() != null) {
            typePredicate = IdPredicate.create(getPredicateVariable().id(getTypeId()), getParentQuery());
        }
        return typePredicate;
    }

    @Nullable
    @Override
    public SchemaConcept getSchemaConcept(){
        if (type == null && getTypeId() != null) {
            SchemaConcept concept = tx().getConcept(getTypeId());
            if (concept == null) throw GraqlQueryException.idNotFound(getTypeId());
            type = concept;
        }
        return type;
    }

    @Override
    public void checkValid() {
        if (getTypePredicate() != null) getTypePredicate().checkValid();
    }

    public boolean isDirect(){
        return getPattern().getProperties(IsaExplicitProperty.class).findFirst().isPresent();
    }

    @Override
    public boolean isAlphaEquivalent(Object obj) {
        if (obj == this) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        Binary that = (Binary) obj;
        return equivalenceBase(that)
                && this.predicateBindingsEquivalent(that, AtomicEquivalence.AlphaEquivalence);
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        if (obj == this) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        Binary that = (Binary) obj;
        return equivalenceBase(that)
                && this.predicateBindingsEquivalent(that, AtomicEquivalence.StructuralEquivalence);
    }

    @Override
    public int alphaEquivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (this.getTypeId() != null? this.getTypeId().hashCode() : 0);
        return hashCode;
    }

    @Override
    public int structuralEquivalenceHashCode() {
        return alphaEquivalenceHashCode();
    }

    private boolean equivalenceBase(Binary that){
        return (this.isUserDefined() == that.isUserDefined())
                && (this.getPredicateVariable().isUserDefinedName() == that.getPredicateVariable().isUserDefinedName())
                && this.isDirect() == that.isDirect()
                && Objects.equals(this.getTypeId(), that.getTypeId());
    }

    boolean predicateBindingsEquivalent(Binary that, AtomicEquivalence equiv) {
        //check if there is a substitution for varName
        IdPredicate thisVarPredicate = this.getIdPredicate(this.getVarName());
        IdPredicate varPredicate = that.getIdPredicate(that.getVarName());

        NeqPredicate thisVarNeqPredicate = this.getPredicates(this.getVarName(), NeqPredicate.class).findFirst().orElse(null);
        NeqPredicate varNeqPredicate = that.getPredicates(that.getVarName(), NeqPredicate.class).findFirst().orElse(null);

        IdPredicate thisTypePredicate = this.getTypePredicate();
        IdPredicate typePredicate = that.getTypePredicate();

        NeqPredicate thisTypeNeqPredicate = this.getPredicates(this.getPredicateVariable(), NeqPredicate.class).findFirst().orElse(null);
        NeqPredicate typeNeqPredicate = that.getPredicates(that.getPredicateVariable(), NeqPredicate.class).findFirst().orElse(null);

        Set<ValuePredicate> thisValuePredicate = this.getPredicates(this.getVarName(), ValuePredicate.class).collect(Collectors.toSet());
        Set<ValuePredicate> valuePredicate = that.getPredicates(that.getVarName(), ValuePredicate.class).collect(Collectors.toSet());

        Set<ValuePredicate> thisTypeValuePredicate  = this.getPredicates(this.getPredicateVariable(), ValuePredicate.class).collect(Collectors.toSet());
        Set<ValuePredicate> typeValuePredicate = that.getPredicates(that.getPredicateVariable(), ValuePredicate.class).collect(Collectors.toSet());

        return ((thisVarPredicate == null && varPredicate == null || thisVarPredicate != null && equiv.equivalent(thisVarPredicate, varPredicate)))
                && (thisVarNeqPredicate == null && varNeqPredicate == null || thisVarNeqPredicate != null && equiv.equivalent(thisVarNeqPredicate, varNeqPredicate))
                && (thisTypePredicate == null && typePredicate == null || thisTypePredicate != null && equiv.equivalent(thisTypePredicate, typePredicate))
                && (thisTypeNeqPredicate == null && typeNeqPredicate == null || thisTypeNeqPredicate != null && equiv.equivalent(thisTypeNeqPredicate, typeNeqPredicate))
                && (thisValuePredicate == null && valuePredicate == null || thisValuePredicate != null && equiv.equivalentCollection(thisValuePredicate, valuePredicate))
                && (thisTypeValuePredicate == null && typeValuePredicate == null || thisTypeValuePredicate != null && equiv.equivalentCollection(thisTypeValuePredicate, typeValuePredicate));
    }

    @Override
    protected Pattern createCombinedPattern(){
        Set<Pattern> vars = Sets.newHashSet((Pattern) getPattern());
        if (getTypePredicate() != null) vars.add(getTypePredicate().getPattern());
        return Graql.and(vars);
    }

    @Override
    public Set<Var> getVarNames() {
        Set<Var> vars = new HashSet<>();
        if (getVarName().isUserDefinedName()) vars.add(getVarName());
        if (getPredicateVariable().isUserDefinedName()) vars.add(getPredicateVariable());
        return vars;
    }

    @Override
    public Stream<Predicate> getInnerPredicates(){
        return getTypePredicate() != null? Stream.of(getTypePredicate()) : Stream.empty();
    }

    @Override
    public Unifier getUnifier(Atom parentAtom, UnifierComparison unifierType) {
        if (!(parentAtom instanceof Binary)) {
            throw GraqlQueryException.unificationAtomIncompatibility();
        }

        boolean inferTypes = unifierType == UnifierType.RULE;
        Var childVarName = this.getVarName();
        Var parentVarName = parentAtom.getVarName();
        Var childPredicateVarName = this.getPredicateVariable();
        Var parentPredicateVarName = parentAtom.getPredicateVariable();
        Type parentType = parentAtom.getParentQuery().getVarTypeMap(inferTypes).get(parentAtom.getVarName());
        Type childType = this.getParentQuery().getVarTypeMap(inferTypes).get(this.getVarName());
        Set<Atomic> parentPredicate = parentAtom.getPredicates(parentVarName, ValuePredicate.class).collect(Collectors.toSet());
        Set<Atomic> childPredicate = this.getPredicates(childVarName, ValuePredicate.class).collect(Collectors.toSet());
        Set<Atomic> parentTypePredicate = parentAtom.getPredicates(parentPredicateVarName, ValuePredicate.class).collect(Collectors.toSet());
        Set<Atomic> childTypePredicate = this.getPredicates(childPredicateVarName, ValuePredicate.class).collect(Collectors.toSet());

        //check for incompatibilities
        if( !unifierType.typeCompatibility(parentAtom.getSchemaConcept(), this.getSchemaConcept())
                || !unifierType.typeCompatibility(parentType, childType)
                || !unifierType.typePlayability(this.getParentQuery(), this.getVarName(), parentType)
                || !unifierType.idCompatibility(parentAtom.getIdPredicate(parentVarName), this.getIdPredicate(childVarName))
                || !unifierType.idCompatibility(parentAtom.getIdPredicate(parentPredicateVarName), this.getIdPredicate(childPredicateVarName))
                || !unifierType.attributeValueCompatibility(parentPredicate, childPredicate)
                || !unifierType.attributeValueCompatibility(parentTypePredicate, childTypePredicate)
                || !unifierType.typeExplicitenessCompatibility(parentAtom, this)){
                     return UnifierImpl.nonExistent();
        }

        Multimap<Var, Var> varMappings = HashMultimap.create();

        if (parentVarName.isUserDefinedName()
                && childVarName.isUserDefinedName()) {
            varMappings.put(childVarName, parentVarName);
        }
        if (parentPredicateVarName.isUserDefinedName()
                && childPredicateVarName.isUserDefinedName()) {
            varMappings.put(childPredicateVarName, parentPredicateVarName);
        }
        return new UnifierImpl(varMappings);
    }
}
