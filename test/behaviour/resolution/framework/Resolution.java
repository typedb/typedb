/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.test.behaviour.resolution.framework;

import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.reference.Reasoner;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.soundness.SoundnessChecker;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import com.vaticle.typeql.lang.query.TypeQLQuery;

import static com.vaticle.typedb.core.TypeDB.Transaction;

public class Resolution {

    private final Reasoner referenceReasoner;

    /**
     * Resolution Testing Framework's entry point. Takes in sessions each for a `Completion` and `Test` keyspace. Each
     * keyspace loaded with the same schema and data. This should be true unless testing this code, in which case a
     * disparity between the two keyspaces is introduced to check that the framework throws an error when it should.
     * @param session TypeDB session, expects schema (including rules) and data to be already present
     */
    public Resolution(RocksSession session) {
        this.referenceReasoner = new Reasoner();
        this.referenceReasoner.run(session);
    }

    /**
     * For each answer to a query, fully explore its explanation to construct a query that will check it was resolved
     * as expected. Run this query on the completion keyspace to verify.
     * @param session TypeDB session, expects schema (including rules) and data to be already present
     * @param inferenceQuery The reference query to make against
     */
    public void testSoundness(RocksSession session, TypeQLMatch inferenceQuery) {
        try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ,
                                                  new Options.Transaction().infer(true).explain(true))) {
            SoundnessChecker soundnessChecker = new SoundnessChecker(referenceReasoner, tx);
            soundnessChecker.check(inferenceQuery);
        }
    }

    /**
     * It is possible that rules could trigger when they should not. Testing for completeness checks the number of
     * inferred facts in the completion keyspace against the total number that are inferred in the test keyspace
     * @param inferenceQuery The reference query to make
     */
    public void testCompleteness(TypeQLMatch inferenceQuery) {
        throw new RuntimeException("Unimplemented");
        // TODO: Bring back completeness check
        // try {
        //     testQuery(TypeQL.parseQuery("match $x isa thing;").asMatch());
        //     testQuery(TypeQL.parseQuery("match $r ($x) isa relation;").asMatch());
        //     testQuery(TypeQL.parseQuery("match $x has attribute $y;").asMatch());
        // } catch (WrongAnswerSizeException ex) {
        //     String msg = String.format("Failed completeness test: [%s]. The complete database contains %d inferred " +
        //                                        "concepts, whereas the test database contains %d inferred concepts.",
        //             ex.getInferenceQuery(), ex.getExpectedAnswers(), ex.getActualAnswers());
        //     throw new CompletenessException(msg);
        // }
    }

}
