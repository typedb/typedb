/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package ai.grakn;

import javax.annotation.Nullable;

/**
 * Enum representing system properties used by Grakn
 *
 * @author Felix Chapman
 */
public enum GraknSystemProperty {

    // TODO: clean and document how these behave and interact
    // what's the difference between grakn.dir and main.basedir? how do they relate to grakn.conf? etc.
    CURRENT_DIRECTORY("grakn.dir"),
    CONFIGURATION_FILE("grakn.conf"),
    TEST_PROFILE("grakn.test-profile"), // Only used in tests
    PROJECT_RELATIVE_DIR("main.basedir"), // Only used in tests
    GRAKN_PID_FILE("grakn.pidfile"),
    ENGINE_JAVAOPTS("engine.javaopts"),
    STORAGE_JAVAOPTS("storage.javaopts");

    private String key;

    GraknSystemProperty(String key) {
        this.key = key;
    }

    /**
     * Return the key identifying the system property
     * @return the key identifying the system property
     */
    public String key() {
        return key;
    }

    /**
     * Retrieve the value of the system property
     * @return the value of the system property, or null if the system property is not set
     */
    @Nullable
    public String value() {
        return System.getProperty(key);
    }

    /**
     * Set the value of the system property
     * @param value the value to set on the system property
     */
    public void set(String value) {
        System.setProperty(key, value);
    }
}
