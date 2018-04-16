/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.pattern.property.IsaExplicitProperty;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;

import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
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
 * @author Kasper Piskorski
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
        return getPattern().admin().getProperties(IsaExplicitProperty.class).findFirst().isPresent();
    }

    @Override
    public boolean isAlphaEquivalent(Object obj) {
        if (obj == this) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        Binary that = (Binary) obj;
        return  (this.isUserDefined() == that.isUserDefined())
                && this.isDirect() == that.isDirect()
                && Objects.equals(this.getTypeId(), that.getTypeId())
                && this.hasEquivalentPredicatesWith(that);
    }

    @Override
    public int alphaEquivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (this.getTypeId() != null? this.getTypeId().hashCode() : 0);
        return hashCode;
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        if (obj == this) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        Binary that = (Binary) obj;
        return  (this.isUserDefined() == that.isUserDefined())
                && this.isDirect() == that.isDirect()
                && Objects.equals(this.getTypeId(), that.getTypeId())
                && this.predicateBindingsAreEquivalent(that);
    }

    @Override
    public int structuralEquivalenceHashCode() {
        return alphaEquivalenceHashCode();
    }

    boolean hasEquivalentPredicatesWith(Binary atom) {
        //check if there is a substitution for varName
        IdPredicate thisVarPredicate = this.getIdPredicate(getVarName());
        IdPredicate varPredicate = atom.getIdPredicate(atom.getVarName());

        IdPredicate thisTypePredicate = this.getTypePredicate();
        IdPredicate typePredicate = atom.getTypePredicate();
        return ((thisVarPredicate == null && varPredicate == null || thisVarPredicate != null && thisVarPredicate.isAlphaEquivalent(varPredicate)))
                && (thisTypePredicate == null && typePredicate == null || thisTypePredicate != null && thisTypePredicate.isAlphaEquivalent(typePredicate));
    }

    boolean predicateBindingsAreEquivalent(Binary atom) {
        //check if there is a substitution for varName
        IdPredicate thisVarPredicate = this.getIdPredicate(getVarName());
        IdPredicate varPredicate = atom.getIdPredicate(atom.getVarName());

        IdPredicate thisTypePredicate = this.getTypePredicate();
        IdPredicate typePredicate = atom.getTypePredicate();
        return (thisVarPredicate == null) == (varPredicate == null)
                && (thisTypePredicate == null) == (typePredicate == null);
    }

    @Override
    protected Pattern createCombinedPattern(){
        Set<PatternAdmin> vars = Sets.newHashSet(getPattern().admin());
        if (getTypePredicate() != null) vars.add(getTypePredicate().getPattern().admin());
        return Patterns.conjunction(vars);
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
    public Unifier getUnifier(Atom parentAtom) {
        if (!(parentAtom instanceof Binary)) {
            throw GraqlQueryException.unificationAtomIncompatibility();
        }

        Multimap<Var, Var> varMappings = HashMultimap.create();
        Var childVarName = this.getVarName();
        Var parentVarName = parentAtom.getVarName();
        Var childPredicateVarName = this.getPredicateVariable();
        Var parentPredicateVarName = parentAtom.getPredicateVariable();

        if (parentVarName.isUserDefinedName()
                && childVarName.isUserDefinedName()
                && !childVarName.equals(parentVarName)) {
            varMappings.put(childVarName, parentVarName);
        }
        if (parentPredicateVarName.isUserDefinedName()
                && childPredicateVarName.isUserDefinedName()
                && !childPredicateVarName.equals(parentPredicateVarName)) {
            varMappings.put(childPredicateVarName, parentPredicateVarName);
        }
        return new UnifierImpl(varMappings);
    }
}
