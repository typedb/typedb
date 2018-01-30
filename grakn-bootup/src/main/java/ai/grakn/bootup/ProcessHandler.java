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

package ai.grakn.bootup;

/**
 * The methods of this interface are used to define a common expectation of what processes should offer.
 * The actual {@link ProcessHandler} interface is never directly accessed.
 * Therefore the need for the suppressions below
 *
 * @author Michele Orsi
 */
interface ProcessHandler {

    @SuppressWarnings("unused")
    void start();

    @SuppressWarnings("unused")
    void stop();

    @SuppressWarnings("unused")
    void status();

    @SuppressWarnings("unused")
    void statusVerbose();

    @SuppressWarnings("unused")
    void clean();

    @SuppressWarnings("unused")
    boolean isRunning();
}
