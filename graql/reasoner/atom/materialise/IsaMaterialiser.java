package grakn.core.graql.reasoner.atom.materialise;

import com.google.common.collect.ImmutableMap;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.graql.reasoner.utils.AnswerUtil;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.graql.reasoner.cache.QueryCache;
import graql.lang.statement.Variable;
import java.util.stream.Stream;

public class IsaMaterialiser implements AtomMaterialiser<IsaAtom>{

    IsaMaterialiser(ReasonerQueryFactory queryFactory, QueryCache queryCache){}

    @Override
    public Stream<ConceptMap> materialise(IsaAtom atom){
        ConceptMap substitution = atom.getParentQuery().getSubstitution();
        EntityType entityType = atom.getSchemaConcept().asEntityType();

        Variable varName = atom.getVarName();
        Concept foundConcept = substitution.containsVar(varName)? substitution.get(varName) : null;
        if (foundConcept != null) return Stream.of(substitution);

        Concept concept = entityType.addEntityInferred();
        return Stream.of(
                AnswerUtil.joinAnswers(substitution, new ConceptMap(ImmutableMap.of(varName, concept))
                ));
    }
}
