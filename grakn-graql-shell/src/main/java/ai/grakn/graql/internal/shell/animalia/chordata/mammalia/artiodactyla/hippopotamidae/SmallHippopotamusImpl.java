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
