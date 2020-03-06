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

package grakn.core.graql.reasoner.cache;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.planning.gremlin.value.ValueOperation;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.utils.AnswerUtil;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.statement.Variable;

import javax.annotation.CheckReturnValue;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Quantifies semantic difference between two queries provided they are in a subsumption relation, i. e. there exists
 * a Unifier of UnifierType#SUBSUMPTIVE between them.
 * Semantic difference between query C and P defines a specialisation operation
 * required to transform query P into a query equivalent to C.
 * In that way we can check whether answers to the parent (more generic) query are also answers
 * to the child query (more specific).
 */
public class SemanticDifference {

    final private ImmutableSet<VariableDefinition> definition;

    public SemanticDifference(Set<VariableDefinition> definition) {
        this.definition = ImmutableSet.copyOf(definition.stream().filter(vd -> !vd.isTrivial()).iterator());
    }

    @Override
    public String toString() {
        return definition.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        SemanticDifference that = (SemanticDifference) obj;
        return this.definition.equals(that.definition);
    }

    @Override
    public int hashCode() {
        return definition.hashCode();
    }

    public boolean isTrivial(){ return definition.isEmpty();}

    private Set<Relation> rolesToRels(Variable var, Set<Role> roles, ConceptMap answer) {
        if (!answer.containsVar(var)) return new HashSet<>();
        Set<Role> roleAndTheirSubs = roles.stream().flatMap(Role::subs).collect(Collectors.toSet());
        return answer.get(var).asThing()
                .relations(roleAndTheirSubs.toArray(new Role[0]))
                .collect(Collectors.toSet());
    }

    private boolean satisfiedBy(ConceptMap answer) {
        if (isEmpty()) return true;

        Map<Variable, Set<Role>> roleRequirements = this.definition.stream()
                .filter(vd -> !vd.playedRoles().isEmpty())
                .collect(Collectors.toMap(VariableDefinition::var, VariableDefinition::playedRoles));

        //check for role compatibility
        Iterator<Map.Entry<Variable, Set<Role>>> reqIterator = roleRequirements.entrySet().iterator();
        Set<Relation> relations;
        if (reqIterator.hasNext()) {
            Map.Entry<Variable, Set<Role>> req = reqIterator.next();
            relations = rolesToRels(req.getKey(), req.getValue(), answer);
        } else {
            relations = new HashSet<>();
        }
        while (!relations.isEmpty() && reqIterator.hasNext()) {
            Map.Entry<Variable, Set<Role>> req = reqIterator.next();
            relations = Sets.intersection(relations, rolesToRels(req.getKey(), req.getValue(), answer));
        }
        if (relations.isEmpty() && !roleRequirements.isEmpty()) return false;

        return definition.stream().allMatch(vd -> {
            Variable var = vd.var();
            Concept concept = answer.get(var);
            if (concept == null) return false;
            Type requiredType = vd.type();
            Role requiredRole = vd.role();
            Type conceptType = requiredType != null? concept.asThing().type() : null;
            Role conceptRole = requiredRole != null? concept.asRole() : null;
            Set<ValuePredicate> vps = vd.valuePredicates();
            return (requiredType == null || requiredType.subs().anyMatch(t -> t.equals(conceptType))) &&
                    (requiredRole == null || requiredRole.subs().anyMatch(r -> r.equals(conceptRole))) &&
                    (vps.isEmpty() || vps.stream().allMatch(
                            vp -> ValueOperation.of(vp.getPredicate()).test(concept.asAttribute().value())
                    ));
        });
    }

    public SemanticDifference merge(SemanticDifference diff) {
        Map<Variable, VariableDefinition> mergedDefinition = definition.stream().collect(Collectors.toMap(VariableDefinition::var, vd -> vd));
        diff.definition.forEach(varDefToMerge -> {
            Variable var = varDefToMerge.var();
            VariableDefinition varDef = mergedDefinition.get(var);
            mergedDefinition.put(var, varDef != null ? varDef.merge(varDefToMerge) : varDefToMerge);
        });
        return new SemanticDifference(new HashSet<>(mergedDefinition.values()));
    }

    /**
     * @param answer to parent query - answer we want to propagate
     * @param childSub partial child substitution that needs to be incorporated
     * @param childVars       child vars
     * @param unifier    parent->child unifier
     * @return propagated answer to child query or empty answer if semantic difference not satisfied
     */
    @CheckReturnValue
    public ConceptMap propagateAnswer(ConceptMap answer, ConceptMap childSub, Set<Variable> childVars, Unifier unifier) {
        if (!this.satisfiedBy(answer)) return new ConceptMap();
        ConceptMap unified = unifier.apply(answer);
        if (unified.isEmpty()) return unified;
        Set<Variable> varsToRetain = Sets.difference(unified.vars(), childSub.vars());
        return AnswerUtil.joinAnswers(unified.project(varsToRetain), childSub).project(childVars);
    }

    boolean isEmpty() { return definition.stream().allMatch(VariableDefinition::isTrivial);}

}
