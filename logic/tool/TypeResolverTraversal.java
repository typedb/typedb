/*
 * Copyright (C) 2020 Grakn Labs
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.logic.tool;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.ConceptManager;
import grakn.core.logic.LogicCache;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;

import java.util.HashMap;
import java.util.Map;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

//TODO: here we remake Type Resolver, using a Traversal Structure instead of a Pattern to move on the graph and find out answers.
public class TypeResolverTraversal {

    private final ConceptManager conceptMgr;
    private final TraversalEngine traversalEng;
    private final LogicCache logicCache;

    public TypeResolverTraversal(ConceptManager conceptMgr, TraversalEngine traversalEng, LogicCache logicCache) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicCache = logicCache;
    }

    public Conjunction resolveVariables(Conjunction conjunction) {
        //TODO: main API
        return conjunction;
    }


    private static class ConstraintMapper {

        private Conjunction conjunction;
        private Map<Identifier, Traversal> resolvers;

        ConstraintMapper(Conjunction conjunction) {
            this.conjunction = conjunction;
            this.resolvers = new HashMap<>();
        }

        private Traversal convert(Variable variable) {
            if (variable.isType()) return convert(variable.asType());
            else if(variable.isThing()) return convert(variable.asThing());
            else throw GraknException.of(ILLEGAL_STATE);
        }

        private Traversal convert(TypeVariable variable) {
            if (resolvers.containsKey(variable.id())) return resolvers.get(variable.id());
//            Traversal resolver = resolvers.
            //TODO
        }

        private Traversal convert(ThingVariable variable) {
            //TODO
        }

    }


}
