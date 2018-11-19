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

package grakn.core.server.keyspace;

import com.google.auto.value.AutoValue;
import grakn.core.common.exception.Validator;
import grakn.core.server.exception.TransactionException;

import javax.annotation.CheckReturnValue;
import java.io.Serializable;

/**
 * <p>
 *     A {@link Keyspace}
 * </p>
 *
 * <p>
 *     A class which represents the unique name of a Grakn Knowledge Base
 * </p>
 *
 */
@AutoValue
public abstract class Keyspace implements Comparable<Keyspace>, Serializable {
    private static final long serialVersionUID = 2726154016735929123L;

    public abstract String getName();

    @Override
    public int compareTo(Keyspace o) {
        if(equals(o)) return 0;
        return getName().compareTo(o.getName());
    }

    /**
     *
     * @param name The string which potentially represents a unique {@link Keyspace}
     * @return The matching {@link Keyspace}
     */
    @CheckReturnValue
    public static Keyspace of(String name){
        if(!Validator.isValidKeyspaceName(name)) {
            throw TransactionException.invalidKeyspaceName(name);
        }
        return new AutoValue_Keyspace(name);
    }

    @Override
    public final String toString() {
        return getName();
    }
}
