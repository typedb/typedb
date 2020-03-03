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
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.utils.AnswerUtil;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.EntityType;
import graql.lang.statement.Variable;

import java.util.stream.Stream;

public class IsaMaterialiser implements AtomMaterialiser<IsaAtom>{

    @Override
    public Stream<ConceptMap> materialise(IsaAtom atom, ReasoningContext ctx){
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
