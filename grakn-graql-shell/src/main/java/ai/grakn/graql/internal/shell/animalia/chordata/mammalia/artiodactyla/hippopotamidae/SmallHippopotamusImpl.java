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

/**
 * A tiny hippo! Isn't it cute?
 *
 * @author Felix Chapman
 */
public class SmallHippopotamusImpl implements Hippopotamus {

    private String me =
            "      >=<    \n" +
            " ,.--'  ''-. \n" +
            " (  )  ',_.' \n" +
            "  Xx'xX      \n";

    @Override
    public void submerge() {
        me = "~~~~~~~~~~~~~ > ";
    }

    @Override
    public boolean isHungryHungry() {
        return false;
    }

    @Override
    public String toString() {
        return me;
    }
}
