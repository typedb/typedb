package grakn.core.graql.reasoner.atom.materialise;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.CacheCasting;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.graql.reasoner.utils.AnswerUtil;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.graql.reasoner.cache.QueryCache;
import graql.lang.statement.Variable;
import java.util.stream.Stream;

public class RelationMaterialiser implements AtomMaterialiser<RelationAtom> {

    private final ReasonerQueryFactory queryFactory;
    private final QueryCache queryCache;

    RelationMaterialiser(ReasonerQueryFactory queryFactory, QueryCache queryCache){
        this.queryFactory = queryFactory;
        this.queryCache = queryCache;
    }

    private Relation findRelation(RelationAtom atom, ConceptMap sub) {
        ReasonerAtomicQuery query = queryFactory.atomic(atom).withSubstitution(sub);
        MultilevelSemanticCache queryCache = CacheCasting.queryCacheCast(this.queryCache);
        ConceptMap answer = queryCache.getAnswerStream(query).findFirst().orElse(null);

        if (answer == null) queryCache.ackDBCompleteness(query);
        return answer != null ? answer.get(atom.getVarName()).asRelation() : null;
    }

    @Override
    public Stream<ConceptMap> materialise(RelationAtom atom) {
        RelationType relationType = atom.getSchemaConcept().asRelationType();
        //in case the roles are variable, we wouldn't have enough information if converted to attribute
        if (relationType.isImplicit()) {
            ConceptMap roleSub = atom.getRoleSubstitution();
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
            Relation foundRelation = findRelation(atom, substitution);
            relation = foundRelation != null? foundRelation : relationType.addRelationInferred();
        }

        //NB: this will potentially reify existing implicit relationships
        roleVarMap.asMap()
                .forEach((key, value) -> value.forEach(var -> relation.assign(key, substitution.get(var).asThing())));

        ConceptMap relationSub = AnswerUtil.joinAnswers(
                atom.getRoleSubstitution(),
                varName.isReturned() ?
                        new ConceptMap(ImmutableMap.of(varName, relation)) :
                        new ConceptMap()
        );

        ConceptMap answer = AnswerUtil.joinAnswers(substitution, relationSub);
        return Stream.of(answer);
    }
}
