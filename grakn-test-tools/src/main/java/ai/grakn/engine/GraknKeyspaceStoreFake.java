/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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
 */

package ai.grakn.engine;

/*-
 * #%L
 * grakn-test-tools
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.Keyspace;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * A fake {@link GraknKeyspaceStore} implementation, that follows the correct contract, but operates without a real
 * knowledge base.
 *
 * @author Felix Chapman
 */
public class GraknKeyspaceStoreFake implements GraknKeyspaceStore {

    private Set<Keyspace> keyspaces;

    private GraknKeyspaceStoreFake(Set<Keyspace> keyspaces) {
        this.keyspaces = keyspaces;
    }

    public static GraknKeyspaceStoreFake of(Keyspace... keyspaces) {
        return new GraknKeyspaceStoreFake(Sets.newHashSet(keyspaces));
    }

    @Override
    public boolean containsKeyspace(Keyspace keyspace) {
        return keyspaces.contains(keyspace);
    }

    @Override
    public boolean deleteKeyspace(Keyspace keyspace) {
        return keyspaces.remove(keyspace);
    }

    @Override
    public Set<Keyspace> keyspaces() {
        return Sets.newHashSet(keyspaces);
    }

    @Override
    public void loadSystemSchema() {

    }

    @Override
    public void addKeyspace(Keyspace keyspace) { keyspaces.add(keyspace); }

    public void clear() {
        keyspaces.clear();
    }
}
