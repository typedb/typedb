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

package grakn.core.server.rpc;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Explanation;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.server.Transaction;
import grakn.protocol.session.AnswerProto;
import grakn.protocol.session.ConceptProto;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Variable;

import java.util.HashMap;
import java.util.Map;

public class RequestReader {

    public static ConceptMap conceptMap(AnswerProto.ConceptMap protoMap, Transaction tx) {
        Map<Variable, Concept> localMap = new HashMap<>();
        protoMap.getMapMap().entrySet().forEach(entry -> {
            Variable var = new Variable(entry.getKey());
            Concept concept = concept(entry.getValue(), tx);
            localMap.put(var, concept);
        });

        Pattern queryPattern = Graql.parsePattern(protoMap.getPattern());
        return new ConceptMap(localMap, null, queryPattern);
    }

    public static Concept concept(ConceptProto.Concept protoConcept, Transaction tx) {
        return tx.getConcept(ConceptId.of(protoConcept.getId()));
    }
}
