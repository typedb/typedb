/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.server.common;

import java.util.UUID;

public class TracingData {

    private final UUID rootID;
    private final UUID parentID;

    public TracingData(String rootID, String parentID) {
        this.rootID = UUID.fromString(rootID);
        this.parentID = UUID.fromString(parentID);
    }

    public UUID rootID() {
        return rootID;
    }

    public UUID parentID() {
        return parentID;
    }
}
