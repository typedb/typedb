/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concept.thing;

import com.vaticle.typedb.core.concept.type.EntityType;

public interface Entity extends Thing {

    /**
     * Get the immediate {@code EntityType} in which this this {@code Entity} is an instance of.
     *
     * @return the {@code EntityType} of this {@code Entity}
     */
    @Override
    EntityType getType();
}
