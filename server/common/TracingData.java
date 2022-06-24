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
