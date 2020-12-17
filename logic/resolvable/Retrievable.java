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

package grakn.core.logic.resolvable;

import grakn.core.pattern.Conjunction;

public class Retrievable extends Resolvable {

    private Conjunction conjunction;

    public Retrievable(Conjunction conjunction) {
        this.conjunction = conjunction;
    }

    @Override
    public Conjunction conjunction() {
        return null; // TODO
    }

    @Override
    public Retrievable asRetrievable() {
        return this;
    }

    @Override
    public boolean isRetrievable() {
        return true;
    }
}
