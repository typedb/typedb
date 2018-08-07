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

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.pattern.property.IsaExplicitProperty;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicEquivalence;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;

import ai.grakn.graql.internal.reasoner.atom.predicate.NeqPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import com.google.common.base.Equivalence;
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
                && this.predicateBindingsEquivalent(that, AtomicEquivalence.AlphaEquivalence);
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
                && this.predicateBindingsEquivalent(that, AtomicEquivalence.StructuralEquivalence);
    }

    @Override
    public int structuralEquivalenceHashCode() {
        return alphaEquivalenceHashCode();
    }

    boolean predicateBindingsEquivalent(Binary that, Equivalence<Atomic> equiv) {
        //check if there is a substitution for varName
        IdPredicate thisVarPredicate = this.getIdPredicate(this.getVarName());
        IdPredicate varPredicate = that.getIdPredicate(that.getVarName());

        NeqPredicate thisVarNeqPredicate = this.getPredicate(this.getVarName(), NeqPredicate.class);
        NeqPredicate varNeqPredicate = that.getPredicate(that.getVarName(), NeqPredicate.class);

        IdPredicate thisTypePredicate = this.getTypePredicate();
        IdPredicate typePredicate = that.getTypePredicate();

        NeqPredicate thisTypeNeqPredicate = this.getPredicate(this.getPredicateVariable(), NeqPredicate.class);
        NeqPredicate typeNeqPredicate = that.getPredicate(that.getPredicateVariable(), NeqPredicate.class);

        return ((thisVarPredicate == null && varPredicate == null || thisVarPredicate != null && equiv.equivalent(thisVarPredicate, varPredicate)))
                && (thisVarNeqPredicate == null && varNeqPredicate == null || thisVarNeqPredicate != null && equiv.equivalent(thisVarNeqPredicate, varNeqPredicate))
                && (thisTypePredicate == null && typePredicate == null || thisTypePredicate != null && equiv.equivalent(thisTypePredicate, typePredicate))
                && (thisTypeNeqPredicate == null && typeNeqPredicate == null || thisTypeNeqPredicate != null && equiv.equivalent(thisTypeNeqPredicate, typeNeqPredicate));
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
