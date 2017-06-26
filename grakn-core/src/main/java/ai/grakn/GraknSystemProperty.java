/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package ai.grakn;

import javax.annotation.Nullable;

/**
 * @author Felix Chapman
 */
public enum GraknSystemProperty {

    CURRENT_DIRECTORY("grakn.dir"),
    CONFIGURATION_FILE("grakn.conf"),
    TEST_PROFILE("grakn.test-profile");

    private String key;

    GraknSystemProperty(String key) {
        this.key = key;
    }

    @Nullable
    public String value() {
        return System.getProperty(key);
    }

    public void set(String value) {
        System.setProperty(key, value);
    }
}
