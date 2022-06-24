/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.test.behaviour.reasoner.verification;

import com.vaticle.typedb.core.database.CoreSession;
import com.vaticle.typeql.lang.query.TypeQLMatch;

public class CorrectnessVerifier {

    private final ForwardChainingMaterialiser materialiser;
    private final CoreSession session;

    private CorrectnessVerifier(CoreSession session, ForwardChainingMaterialiser materialiser) {
        this.session = session;
        this.materialiser = materialiser;
    }

    public static CorrectnessVerifier initialise(CoreSession session) {
        return new CorrectnessVerifier(session, ForwardChainingMaterialiser.materialise(session));
    }

    public void verifyCorrectness(TypeQLMatch inferenceQuery) {
        verifySoundness(inferenceQuery);
        verifyCompleteness(inferenceQuery);
    }

    public void verifySoundness(TypeQLMatch inferenceQuery) {
        SoundnessVerifier.create(materialiser, session).verifyQuery(inferenceQuery);
    }

    public void verifyCompleteness(TypeQLMatch inferenceQuery) {
        CompletenessVerifier.create(materialiser, session).verifyQuery(inferenceQuery);
    }

    public void close() {
        materialiser.close();
    }

    public static class CompletenessException extends RuntimeException {
        CompletenessException(String message) {
            super(message);
        }
    }

    public static class SoundnessException extends RuntimeException {
        SoundnessException(String message) {
            super(message);
        }
    }
}
