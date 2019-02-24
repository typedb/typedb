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

package grakn.core.graql.internal.reasoner.cache;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.Concept;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.Type;
import grakn.core.graql.internal.executor.property.ValueExecutor;
import grakn.core.graql.internal.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.internal.reasoner.unifier.Unifier;
import grakn.core.graql.internal.reasoner.unifier.UnifierType;
import grakn.core.server.kb.concept.ConceptUtils;
import graql.lang.statement.Variable;

import javax.annotation.CheckReturnValue;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Quantifies semantic difference between two queries provided they are in a subsumption relation, i. e. there exists
 * a {@link Unifier} of {@link UnifierType#SUBSUMPTIVE} between them.
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

    private Set<Relation> rolesToRels(Variable var, Set<Role> roles, ConceptMap answer) {
        if (!answer.containsVar(var)) return new HashSet<>();
        Set<Role> roleAndTheirSubs = roles.stream().flatMap(Role::subs).collect(Collectors.toSet());
        return answer.get(var).asThing()
                .relations(roleAndTheirSubs.toArray(new Role[0]))
                .collect(Collectors.toSet());
    }

    public boolean satisfiedBy(ConceptMap answer) {
        if (isEmpty()) return true;

        Map<Variable, Set<Role>> roleRequirements = this.definition.stream()
                .filter(vd -> !vd.playedRoles().isEmpty())
                .collect(Collectors.toMap(VariableDefinition::var, VariableDefinition::playedRoles));

        //check for role compatibility
        Iterator<Map.Entry<Variable, Set<Role>>> reqIterator = roleRequirements.entrySet().iterator();
        Set<Relation> relationships;
        if (reqIterator.hasNext()) {
            Map.Entry<Variable, Set<Role>> req = reqIterator.next();
            relationships = rolesToRels(req.getKey(), req.getValue(), answer);
        } else {
            relationships = new HashSet<>();
        }
        while (!relationships.isEmpty() && reqIterator.hasNext()) {
            Map.Entry<Variable, Set<Role>> req = reqIterator.next();
            relationships = Sets.intersection(relationships, rolesToRels(req.getKey(), req.getValue(), answer));
        }
        if (relationships.isEmpty() && !roleRequirements.isEmpty()) return false;

        return definition.stream().allMatch(vd -> {
            Variable var = vd.var();
            Concept concept = answer.get(var);
            if (concept == null) return false;
            Type type = vd.type();
            Role role = vd.role();
            Set<ValuePredicate> vps = vd.valuePredicates();
            return (type == null || type.subs().anyMatch(t -> t.equals(concept.asThing().type()))) &&
                    (role == null || role.subs().anyMatch(r -> r.equals(concept.asRole()))) &&
                    (vps.isEmpty() || vps.stream().allMatch(
                            vp -> ValueExecutor.Operation.of(vp.getPredicate()).test(concept.asAttribute().value())
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
     * @param answer to project
     * @param partialSub partial child substitution that needs to be incorporated
     * @param vars       child vars
     * @param unifier    parent-child unifier
     * @return projected answer (empty if semantic difference not satisfied)
     */
    @CheckReturnValue
    public ConceptMap applyToAnswer(ConceptMap answer, ConceptMap partialSub, Set<Variable> vars, Unifier unifier) {
        ConceptMap unified = unifier.apply(answer);
        if (unified.isEmpty()) return unified;
        Set<Variable> varsToRetain = Sets.difference(unified.vars(), partialSub.vars());
        return !this.satisfiedBy(unified) ? new ConceptMap() :
                ConceptUtils.mergeAnswers(unified.project(varsToRetain), partialSub).project(vars);
    }

    boolean isEmpty() { return definition.stream().allMatch(VariableDefinition::isTrivial);}

}
