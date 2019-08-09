/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graql.reasoner.atom.binary;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import grakn.core.concept.ConceptId;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.concept.type.Type;
import grakn.core.graql.exception.GraqlCheckedException;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.atom.predicate.Predicate;
import grakn.core.graql.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.reasoner.unifier.Unifier;
import grakn.core.graql.reasoner.unifier.UnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierType;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.property.IsaProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;

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

    public abstract Variable getPredicateVariable();
    @Nullable @Override public abstract ConceptId getTypeId();

    private SchemaConcept type = null;
    private IdPredicate typePredicate = null;

    @Nullable
    public IdPredicate getTypePredicate(){
        if (typePredicate == null && getTypeId() != null) {
            typePredicate = IdPredicate.create(new Statement(getPredicateVariable()).id(getTypeId().getValue()), getParentQuery());
        }
        return typePredicate;
    }

    public boolean isDirect(){
        return getPattern().getProperties(IsaProperty.class).findFirst()
                .map(IsaProperty::isExplicit).orElse(false);
    }

    @Nullable
    @Override
    public SchemaConcept getSchemaConcept(){
        if (type == null && getTypeId() != null) {
            SchemaConcept concept = tx().getConcept(getTypeId());
            if (concept == null) throw GraqlCheckedException.idNotFound(getTypeId());
            type = concept;
        }
        return type;
    }

    @Override
    public void checkValid() {
        if (getTypePredicate() != null) getTypePredicate().checkValid();
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
        int hashCode = 1;
        hashCode = hashCode * 37 + (this.getTypeId() != null? this.getTypeId().hashCode() : 0);
        return hashCode;
    }

    @Override
    public int structuralEquivalenceHashCode() {
        return alphaEquivalenceHashCode();
    }

    boolean isBaseEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Binary that = (Binary) obj;
        return (this.isUserDefined() == that.isUserDefined())
                && (this.getPredicateVariable().isReturned() == that.getPredicateVariable().isReturned())
                && this.isDirect() == that.isDirect()
                && Objects.equals(this.getTypeId(), that.getTypeId());
    }

    @Override
    protected Pattern createCombinedPattern(){
        Set<Pattern> vars = Sets.newHashSet((Pattern) getPattern());
        if (getTypePredicate() != null) vars.add(getTypePredicate().getPattern());
        return Graql.and(vars);
    }

    @Override
    public Set<Variable> getVarNames() {
        Set<Variable> vars = new HashSet<>();
        if (getVarName().isReturned()) vars.add(getVarName());
        if (getPredicateVariable().isReturned()) vars.add(getPredicateVariable());
        return vars;
    }

    @Override
    public Stream<Predicate> getInnerPredicates(){
        return getTypePredicate() != null? Stream.of(getTypePredicate()) : Stream.empty();
    }

    @Override
    public Unifier getUnifier(Atom parentAtom, UnifierType unifierType) {
        boolean inferTypes = unifierType.inferTypes();
        Variable childVarName = this.getVarName();
        Variable parentVarName = parentAtom.getVarName();
        Variable childPredicateVarName = this.getPredicateVariable();
        Variable parentPredicateVarName = parentAtom.getPredicateVariable();
        Set<Type> parentTypes = parentAtom.getParentQuery().getVarTypeMap(inferTypes).get(parentAtom.getVarName());
        Set<Type> childTypes = this.getParentQuery().getVarTypeMap(inferTypes).get(this.getVarName());

        SchemaConcept parentType = parentAtom.getSchemaConcept();
        SchemaConcept childType = this.getSchemaConcept();

        //check for incompatibilities
        if( !unifierType.typeCompatibility(
                parentType != null? Collections.singleton(parentType) : Collections.emptySet(),
                childType != null? Collections.singleton(childType) : Collections.emptySet())
                || !unifierType.typeCompatibility(parentTypes, childTypes)
                || !parentTypes.stream().allMatch(pType -> unifierType.typePlayability(this.getParentQuery(), this.getVarName(), pType))
                || !unifierType.typeDirectednessCompatibility(parentAtom, this)){
                     return UnifierImpl.nonExistent();
        }

        Multimap<Variable, Variable> varMappings = HashMultimap.create();

        if (parentVarName.isReturned()) {
            varMappings.put(childVarName, parentVarName);
        }
        if (parentPredicateVarName.isReturned()) {
            varMappings.put(childPredicateVarName, parentPredicateVarName);
        }

        UnifierImpl unifier = new UnifierImpl(varMappings);
        return isPredicateCompatible(parentAtom, unifier, unifierType)?
                unifier : UnifierImpl.nonExistent();
    }
}
