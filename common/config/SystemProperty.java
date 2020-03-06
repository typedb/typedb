/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.common.config;

import javax.annotation.Nullable;

/**
 * Enum representing system properties used by Grakn
 */
public enum SystemProperty {

    CURRENT_DIRECTORY("grakn.dir"),
    CONFIGURATION_FILE("grakn.conf"),
    GRAKN_PID_FILE("grakn.pidfile"),
    SERVER_JAVAOPTS("server.javaopts"),
    STORAGE_JAVAOPTS("storage.javaopts");

    private String key;

    SystemProperty(String key) {
        this.key = key;
    }

    /**
     * Return the key identifying the system property
     *
     * @return the key identifying the system property
     */
    public String key() {
        return key;
    }

    /**
     * Retrieve the value of the system property
     *
     * @return the value of the system property, or null if the system property is not set
     */
    @Nullable
    public String value() {
        return System.getProperty(key);
    }

    /**
     * Set the value of the system property
     *
     * @param value the value to set on the system property
     */
    public void set(String value) {
        System.setProperty(key, value);
    }
}
