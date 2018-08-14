/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
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
 */

package pick;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.answer.Value;

import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.count;

/**
 *
 */
public class ConceptIdPicker extends Picker<ConceptId> {

    protected Pattern matchVarPattern;
    protected Var matchVar;

    public ConceptIdPicker(Random rand, Pattern matchVarPattern, Var matchVar) {
        super(rand);
        this.matchVarPattern = matchVarPattern;
        this.matchVar = matchVar;
    }

    /**
     * @param streamLength
     * @param tx
     * @return
     */
    @Override
    public Stream<ConceptId> getStream(int streamLength, GraknTx tx) {

        Stream<Integer> randomUniqueOffsetStream = this.getRandomOffsetStream(streamLength, tx);
        if (randomUniqueOffsetStream == null ) {
            return Stream.empty();
        }

        return randomUniqueOffsetStream.map(randomOffset -> {

            QueryBuilder qb = tx.graql();

            // TODO The following gives exception: Exception in thread "main" java.lang.ClassCastException: ai.grakn.remote.RemoteGraknTx cannot be cast to ai.grakn.kb.internal.EmbeddedGraknTx
            // TODO Waiting on bug fix
//            Stream<Concept> resultStream = qb.match(this.matchVarPattern)
//                    .offset(randomOffset)
//                    .limit(1)
//                    .get(this.matchVar);

            List<ConceptMap> result = qb.match(this.matchVarPattern)
                    .offset(randomOffset)
                    .limit(1)
                    .get()
                    .execute();

            // Because we use limit 1, there will only be 1 result
            // return the ConceptId of the single variable in the single result
            return result.get(0).get(this.matchVar).id();
        });
    }

    /**
     * @param tx
     * @return
     */
    protected Integer getConceptCount(GraknTx tx) {
        QueryBuilder qb = tx.graql();
        // TODO This isn't working, waiting on bug fix - this was likely due to a mismatch between the Grakn active code and the Grakn build on my machine. Test to check if this is still a problem
        Value count_value = qb.match(this.matchVarPattern).aggregate(count()).execute().get(0);
        Number count = count_value.number();
        return Math.toIntExact((long)count);
    }
}
