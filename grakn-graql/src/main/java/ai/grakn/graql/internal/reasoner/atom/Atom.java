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
package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.GraknGraph;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.Reasoner;
import ai.grakn.graql.internal.reasoner.atom.binary.Binary;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import javafx.util.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * <p>
 * Atom implementation defining specialised functionalities.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class Atom extends AtomBase {

    protected Type type = null;
    protected ConceptId typeId = null;

    protected Atom(VarAdmin pattern) { this(pattern, null);}
    protected Atom(VarAdmin pattern, Query par) { super(pattern, par);}
    protected Atom(Atom a) {
        super(a);
        this.type = a.type;
        this.typeId = a.getTypeId() != null? ConceptId.of(a.getTypeId().getValue()) : null;
    }

    @Override
    public boolean isAtom(){ return true;}

    public boolean isBinary(){return false;}

    /**
     * @return true if the atom corresponds to a atom
     * */
    public boolean isType(){ return false;}

    /**
     * @return true if the atom corresponds to a non-unary atom
     * */
    public boolean isRelation(){return false;}

    /**
     * @return true if the atom corresponds to a resource atom
     * */
    public boolean isResource(){ return false;}

    protected abstract boolean isRuleApplicable(InferenceRule child);

    public Set<Rule> getApplicableRules() {
        Set<Rule> children = new HashSet<>();
        GraknGraph graph = getParentQuery().graph();
        Collection<Rule> rulesFromType = getType() != null? getType().getRulesOfConclusion() : Reasoner.getRules(graph);
        rulesFromType.forEach(rule -> {
            InferenceRule child = new InferenceRule(rule, graph);
            boolean ruleRelevant = isRuleApplicable(child);
            if (ruleRelevant) children.add(rule);
        });
        return children;
    }

    @Override
    public boolean isRuleResolvable() {
        Type type = getType();
        return type != null
                && !type.getRulesOfConclusion().isEmpty()
                && !this.getApplicableRules().isEmpty();
    }

    @Override
    public boolean isRecursive(){
        if (isResource() || getType() == null) return false;
        boolean atomRecursive = false;

        Type type = getType();
        Collection<Rule> presentInConclusion = type.getRulesOfConclusion();
        Collection<Rule> presentInHypothesis = type.getRulesOfHypothesis();

        for(Rule rule : presentInConclusion)
            atomRecursive |= presentInHypothesis.contains(rule);
        return atomRecursive;
    }

    /**
     * @return true if the atom requires materialisation in order to be referenced
     */
    public boolean requiresMaterialisation(){ return false; }

    /**
     * @return corresponding type if any
     */
    public Type getType(){
        if (type == null && typeId != null)
            type = getParentQuery().graph().getConcept(typeId).asType();
        return type;
    }

    /**
     * @return type id of the corresponding type if any
     */
    public ConceptId getTypeId(){ return typeId;}

    /**
     * @return value variable name
     */
    public VarName getValueVariable() {
        throw new IllegalArgumentException("getValueVariable called on Atom object " + getPattern());
    }

    /**
     * @return set of predicates relevant to this atom
     */
    public abstract Set<Predicate> getPredicates();

    /**
     * @return set of id predicates relevant to this atom
     */
    public abstract Set<Predicate> getIdPredicates();

    /**
     * @return set of value predicates relevant to this atom
     */
    public abstract Set<Predicate> getValuePredicates();


    /**
     * @return set of types relevant to this atom
     */
    public Set<Atom> getTypeConstraints(){
        Set<Atom> relevantTypes = new HashSet<>();
        //ids from indirect types
        getParentQuery().getTypeConstraints().stream()
                .filter(atom -> containsVar(atom.getVarName()))
                .forEach(atom -> {
                    relevantTypes.add(atom);
                    relevantTypes.addAll(((Binary)atom).getLinkedAtoms());
                });
        return relevantTypes;
    }

    /**
     * @return map of varName-(var type, var role type) pairs
     */
    public Map<VarName, javafx.util.Pair<Type, RoleType>> getVarTypeRoleMap() {
        Map<VarName, javafx.util.Pair<Type, RoleType>> roleVarTypeMap = new HashMap<>();
        if (getParentQuery() == null) return roleVarTypeMap;
        Set<VarName> vars = getVarNames();
        Map<VarName, Type> varTypeMap = getParentQuery().getVarTypeMap();

        vars.forEach(var -> {
            Type type = varTypeMap.get(var);
            roleVarTypeMap.put(var, new Pair<>(type, null));
        });
        return roleVarTypeMap;
    }

    /**
     * @return map of role type- (var name, var type) pairs
     */
    public Map<RoleType, Pair<VarName, Type>> getRoleVarTypeMap() { return new HashMap<>();}

    /**
     * infers types (type, role types) fo the atom if applicable/possible
     */
    public void inferTypes(){}

    /**
     * rewrites the atom to be compatible with parent atom
     * @param parent atom to be compatible with
     * @param q query the rewritten atom should belong to
     * @return pair of (rewritten atom, unifiers required to unify child with rewritten atom)
     */
    public Pair<Atom, Map<VarName, VarName>> rewrite(Atom parent, Query q){ return new Pair<>(this, new HashMap<>());}
}
