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

package ai.grakn.graql.internal.shell.animalia.chordata.mammalia.artiodactyla.hippopotamidae;

public class HippopotamusFactory {

    private int size = 10;

    public static HippopotamusFactory builder() {
        return new HippopotamusFactory();
    }

    public HippopotamusFactory size(int size) {
        this.size = size;
        return this;
    }

    public Hippopotamus build() {
        if (size < 5) {
            return new SmallHippopotamusImpl();
        } else if (size < Integer.MAX_VALUE) {
            return new LargeHippopotamusImpl();
        } else {
            return new TitanicHippopotamusImpl();
        }
    }
}
