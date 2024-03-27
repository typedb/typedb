/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.test.behaviour.reasoner.verification;

import com.vaticle.typedb.core.database.CoreSession;
import com.vaticle.typeql.lang.query.TypeQLGet;

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

    public void verifyCorrectness(TypeQLGet inferenceQuery) {
        verifySoundness(inferenceQuery);
        verifyCompleteness(inferenceQuery);
    }

    public void verifySoundness(TypeQLGet inferenceQuery) {
        SoundnessVerifier.create(materialiser, session).verifyQuery(inferenceQuery);
    }

    public void verifyCompleteness(TypeQLGet inferenceQuery) {
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
