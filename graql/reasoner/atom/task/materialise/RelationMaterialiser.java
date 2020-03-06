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

package grakn.core.graql.reasoner.atom.task.materialise;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.CacheCasting;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.utils.AnswerUtil;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.manager.ConceptManager;
import graql.lang.statement.Variable;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class RelationMaterialiser implements AtomMaterialiser<RelationAtom> {

    @Override
    public Stream<ConceptMap> materialise(RelationAtom atom, ReasoningContext ctx) {
        RelationType relationType = atom.getSchemaConcept().asRelationType();
        //in case the roles are variable, we wouldn't have enough information if converted to attribute
        if (relationType.isImplicit()) {
            ConceptMap roleSub = getRoleSubstitution(atom, ctx);
            return atom.toAttributeAtom().materialise().map(ans -> AnswerUtil.joinAnswers(ans, roleSub));
        }
        Multimap<Role, Variable> roleVarMap = atom.getRoleVarMap();
        ConceptMap substitution = atom.getParentQuery().getSubstitution();

        //NB: if the relation is implicit, it will be created as a reified relation
        //if the relation already exists, only assign roleplayers, otherwise create a new relation
        Relation relation;
        Variable varName = atom.getVarName();
        if (substitution.containsVar(varName)) {
            relation = substitution.get(varName).asRelation();
        } else {
            Relation foundRelation = findRelation(atom, substitution, ctx);
            relation = foundRelation != null? foundRelation : relationType.addRelationInferred();
        }

        //NB: this will potentially reify existing implicit relationships
        roleVarMap.asMap()
                .forEach((key, value) -> value.forEach(var -> relation.assign(key, substitution.get(var).asThing())));

        ConceptMap relationSub = AnswerUtil.joinAnswers(
                getRoleSubstitution(atom, ctx),
                varName.isReturned() ?
                        new ConceptMap(ImmutableMap.of(varName, relation)) :
                        new ConceptMap()
        );

        ConceptMap answer = AnswerUtil.joinAnswers(substitution, relationSub);
        return Stream.of(answer);
    }

    private Relation findRelation(RelationAtom atom, ConceptMap sub, ReasoningContext ctx) {
        ReasonerAtomicQuery query = ctx.queryFactory().atomic(atom).withSubstitution(sub);
        MultilevelSemanticCache queryCache = CacheCasting.queryCacheCast(ctx.queryCache());
        ConceptMap answer = queryCache.getAnswerStream(query).findFirst().orElse(null);

        if (answer == null) queryCache.ackDBCompleteness(query);
        return answer != null ? answer.get(atom.getVarName()).asRelation() : null;
    }

    private ConceptMap getRoleSubstitution(RelationAtom atom, ReasoningContext ctx) {
        Map<Variable, Concept> roleSub = new HashMap<>();
        ConceptManager conceptManager = ctx.conceptManager();
        atom.getRolePredicates(conceptManager).forEach(p -> roleSub.put(p.getVarName(), conceptManager.getConcept(p.getPredicate())));
        return new ConceptMap(roleSub);
    }
}
