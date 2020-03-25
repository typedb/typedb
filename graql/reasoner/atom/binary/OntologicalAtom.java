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
import grakn.common.util.Pair;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.rule.InferenceRule;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.property.HasAttributeTypeProperty;
import graql.lang.property.PlaysProperty;
import graql.lang.property.RelatesProperty;
import graql.lang.property.SubProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import static graql.lang.Graql.type;
import static graql.lang.Graql.var;

/**
 * Class for defining different ontological Atoms - ones referring to ontological elements.
 */
public class OntologicalAtom extends TypeAtom {

    public enum OntologicalAtomType{
        HasAtom(HasAttributeTypeProperty.class, (v, label) -> var(v.first()).has(type(label.getValue()))),
        PlaysAtom(PlaysProperty.class, (v, label) -> var(v.first()).plays(var(v.second()))),
        SubAtom(SubProperty.class, (v, label) -> var(v.first()).sub(var(v.second()))),
        SubDirectAtom(SubProperty.class, (v, label) -> var(v.first()).subX(var(v.second()))),
        RelatesAtom(RelatesProperty.class, (v, label) -> var(v.first()).relates(var(v.second())));

        private final Class<? extends VarProperty> property;
        private final BiFunction<Pair<Variable, Variable>, Label, Statement> statementFunction;

        OntologicalAtomType(Class<? extends VarProperty> property, BiFunction<Pair<Variable, Variable>, Label, Statement> statementFunction){
            this.property = property;
            this.statementFunction = statementFunction;
        }

        public Class<? extends VarProperty> property(){ return property;}
        public BiFunction<Pair<Variable, Variable>, Label, Statement> statementFunction(){ return statementFunction;}
    }

    private final OntologicalAtomType atomType;

    private OntologicalAtom(Variable varName, Statement pattern, ReasonerQuery reasonerQuery, @Nullable Label label,
                            Variable predicateVariable, OntologicalAtomType type, ReasoningContext ctx) {
        super(varName, pattern, reasonerQuery, label, predicateVariable, ctx);
        this.atomType = type;
    }

    public static OntologicalAtom create(Variable var, Variable pVar, @Nullable Label label, ReasonerQuery parent, OntologicalAtomType type,
                                         ReasoningContext ctx) {
        Variable varName = var.asReturnedVar();
        Variable predicateVar = pVar.asReturnedVar();
        Statement statement = type.statementFunction().apply(new Pair<>(var, pVar), label);
        return new OntologicalAtom(varName, statement, parent, label, predicateVar, type, ctx);
    }

    private static OntologicalAtom create(OntologicalAtom a, ReasonerQuery parent) {
        return create(a.getVarName(), a.getPredicateVariable(), a.getTypeLabel(), parent, a.atomType(), a.context());
    }

    @Override
    public Atomic copy(ReasonerQuery parent){
        return create(this, parent);
    }

    public OntologicalAtomType atomType(){ return atomType;}

    @Override
    public String toString() {
        return getPattern().toString() +
                (getTypeLabel() != null ? getTypeLabel() : "");
    }

    @Override
    public boolean isSelectable() {
        return true;
    }

    @Override
    public boolean isSubsumedBy(Atomic atom) {
        return this.isAlphaEquivalent(atom);
    }

    @Override
    public Stream<Rule> getPotentialRules() {
        return Stream.empty();
    }

    @Override
    public Stream<InferenceRule> getApplicableRules() {
        return Stream.empty();
    }

    @Override
    public Set<String> validateAsRuleHead(Rule rule) {
        return Sets.newHashSet(ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD.getMessage(rule.then(), rule.label()));
    }

    @Override
    public Set<TypeAtom> unify(Unifier u) {
        Collection<Variable> vars = u.get(getVarName());
        return vars.isEmpty() ?
                Collections.singleton(this) :
                vars.stream().map(v -> create(v, getPredicateVariable(), getTypeLabel(), this.getParentQuery(), atomType(), context())).collect(Collectors.toSet());
    }

    @Override
    public Atom rewriteWithTypeVariable() {
        return create(getVarName(), getPredicateVariable().asReturnedVar(), getTypeLabel(), getParentQuery(), atomType(), context());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom) {
        return parentAtom.getPredicateVariable().isReturned() ?
                create(getVarName(), getPredicateVariable().asReturnedVar(), getTypeLabel(), getParentQuery(), atomType(), context()) :
                this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OntologicalAtom that = (OntologicalAtom) o;
        return (this.getVarName().equals(that.getVarName()))
                && ((this.getTypeLabel() == null) ? (that.getTypeLabel() == null) : this.getTypeLabel().equals(that.getTypeLabel()))
                && atomType == that.atomType;
    }

    @Override
    public boolean isAlphaEquivalent(Object obj) {
        if (!super.isAlphaEquivalent(obj)) return false;
        OntologicalAtom that = (OntologicalAtom) obj;
        return atomType == that.atomType;
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        if (!super.isStructurallyEquivalent(obj)) return false;
        OntologicalAtom that = (OntologicalAtom) obj;
        return atomType == that.atomType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getVarName(), atomType);
    }

    @Override
    public int alphaEquivalenceHashCode() {
        return Objects.hash(getTypeLabel());
    }

    @Override
    public int structuralEquivalenceHashCode() {
        return alphaEquivalenceHashCode();
    }

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() {return atomType.property();}
}
