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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import grakn.core.graql.Var;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.Relationship;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.internal.reasoner.unifier.UnifierType;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Quantifies semantic difference between two queries provided they are in a subsumption relation, i. e. there exists
 * a {@link Unifier} of {@link UnifierType#SUBSUMPTIVE} between them.
 *
 * Semantic difference between query C and P defines a specialisation operation
 * required to transform query P into a query equivalent to C.
 *
 * In that way we can check whether answers to the parent (more generic) query are also answers
 * to the child query (more specific).
 *
 * @author Kasper Piskorski
 *
 */
public class SemanticDifference {

    //TODO define as a set of VariableDefinitions with def holding var
    final private ImmutableMap<Var, VariableDefinition> definition;

    public SemanticDifference(Map<Var, VariableDefinition> m){
        this(m.entrySet());
    }

    private SemanticDifference(Collection<Map.Entry<Var, VariableDefinition>> mappings){
        ImmutableMap.Builder<Var, VariableDefinition> builder = ImmutableMap.builder();
        mappings.stream().filter(e -> !e.getValue().isTrivial()).forEach(e -> builder.put(e.getKey(), e.getValue()));
        this.definition = builder.build();
    }

    @Override
    public String toString(){
        return definition.toString();
    }

    @Override
    public boolean equals(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        SemanticDifference that = (SemanticDifference) obj;
        return this.definition.equals(that.definition);
    }

    @Override
    public int hashCode(){
        return definition.hashCode();
    }

    private Set<Relationship> rolesToRels(Var var, Set<Role> roles, ConceptMap answer){
        if(!answer.containsVar(var)) return new HashSet<>();
        Set<Role> roleAndTheirSubs = roles.stream().flatMap(Role::subs).collect(Collectors.toSet());
        return answer.get(var).asThing()
                .relationships(roleAndTheirSubs.toArray(new Role[roleAndTheirSubs.size()]))
                .collect(Collectors.toSet());
    }

    public boolean satisfiedBy(ConceptMap answer){
        if (isEmpty()) return true;

        Map<Var, Set<Role>> roleRequirements = this.definition.entrySet().stream()
                .filter(entry -> !entry.getValue().playedRoles().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().playedRoles()));

        //check for role compatibility
        Iterator<Map.Entry<Var, Set<Role>>> reqIterator = roleRequirements.entrySet().iterator();
        Set<Relationship> relationships;
        if (reqIterator.hasNext()){
            Map.Entry<Var, Set<Role>> req = reqIterator.next();
            relationships = rolesToRels(req.getKey(), req.getValue(), answer);
        } else {
            relationships = new HashSet<>();
        }
        while(!relationships.isEmpty() && reqIterator.hasNext()){
            Map.Entry<Var, Set<Role>> req = reqIterator.next();
            relationships = Sets.intersection(relationships, rolesToRels(req.getKey(), req.getValue(), answer));
        }
        if(relationships.isEmpty() && !roleRequirements.isEmpty()) return false;

        //TODO iterate over definition instead of answer
        return answer.map().entrySet().stream().allMatch(entry -> {
            Var ansVar = entry.getKey();
            Concept concept = entry.getValue();
            VariableDefinition varDef = definition.get(ansVar);
            if (varDef == null) return true;
            Type type = varDef.type();
            Role role = varDef.role();
            Set<ValuePredicate> vps = varDef.valuePredicates();

            return (type == null || type.subs().anyMatch(t -> t.equals(concept.asThing().type())))
                    && (role == null || role.subs().anyMatch(r -> r.equals(concept.asRole())))
                    && (vps.isEmpty() || vps.stream().allMatch(vp -> vp.getPredicate().getPredicate().get().test(concept.asAttribute().value())));
        });
    }

    public SemanticDifference merge(SemanticDifference diff){
        Map<Var, VariableDefinition> mergedDefinition = new HashMap<>(definition);
        diff.definition.forEach((var, varDefToMerge) -> {
            VariableDefinition varDef = mergedDefinition.get(var);
            mergedDefinition.put(var, varDef != null ? varDef.merge(varDefToMerge) : varDefToMerge);
        });
        return new SemanticDifference(mergedDefinition);
    }

    boolean isEmpty(){ return definition.values().stream().allMatch(VariableDefinition::isTrivial);}

}
