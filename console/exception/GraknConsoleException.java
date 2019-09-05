/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.console.exception;

import grakn.client.exception.GraknClientException;

public class GraknConsoleException extends GraknClientException {

    protected GraknConsoleException(String error) {
        super(error);
    }

    protected GraknConsoleException(String error, RuntimeException e) {
        super(error, e);
    }

    public static GraknConsoleException create(String error) {
        return new GraknConsoleException(error);
    }

    public static GraknConsoleException create(String error, RuntimeException e) {
        return new GraknConsoleException(error, e);
    }

    public static GraknConsoleException unreachableServer(String serverAddress, RuntimeException e) {
        return GraknConsoleException.create("Unable to create connection to Grakn instance at " + serverAddress, e);
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }
}
