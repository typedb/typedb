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
 *
 */

package grakn.core.server.keyspace;

import grakn.core.kb.server.exception.TransactionException;
import grakn.core.kb.server.keyspace.Keyspace;

import javax.annotation.CheckReturnValue;
import java.io.Serializable;
import java.util.regex.Pattern;


/**
 * An identifier for an isolated scope of a data in the database.
 */
public class KeyspaceImpl implements Serializable, Comparable<KeyspaceImpl>, Keyspace {
    private static final long serialVersionUID = 2726154016735929123L;
    private static final int MAX_LENGTH = 48;

    private final String name;

    public KeyspaceImpl(String name) {
        if (name == null) {
            throw new NullPointerException("Null name");
        }
        if (!isValidName(name)) {
            throw TransactionException.invalidKeyspaceName(name);
        }
        this.name = name;
    }

    @Override
    @CheckReturnValue
    public String name() {
        return name;
    }

    @Override
    public final String toString() {
        return name();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyspaceImpl that = (KeyspaceImpl) o;
        return this.name.equals(that.name());
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.name.hashCode();
        return h;
    }

    @Override
    public int compareTo(KeyspaceImpl o) {
        if (equals(o)) return 0;
        return name().compareTo(o.name());
    }

    private static boolean isValidName(String name) {
        return Pattern.matches("[a-z_][a-z_0-9]*", name) && name.length() <= MAX_LENGTH;
    }
}
