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

import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.completeness.CompletenessChecker;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.reference.Reasoner;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.soundness.SoundnessChecker;
import com.vaticle.typeql.lang.query.TypeQLMatch;

public class CorrectnessChecker {

    private final Reasoner referenceReasoner;
    private final RocksSession session;

    public CorrectnessChecker(RocksSession session, Reasoner referenceReasoner) {
        this.session = session;
        this.referenceReasoner = referenceReasoner;
    }

    public static CorrectnessChecker initialise(RocksSession session) {
        return new CorrectnessChecker(session, Reasoner.runRules(session));
    }

    public void checkSoundness(RocksSession session, TypeQLMatch inferenceQuery) {
        SoundnessChecker.create(referenceReasoner, session).checkQuery(inferenceQuery);
    }

    public void checkCompleteness(TypeQLMatch inferenceQuery) {
        CompletenessChecker.create(referenceReasoner, session).checkQuery(inferenceQuery);
    }

    public void close() {
        referenceReasoner.close();
    }

}
