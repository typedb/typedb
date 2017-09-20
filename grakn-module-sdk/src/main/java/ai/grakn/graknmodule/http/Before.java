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

package ai.grakn.graknmodule.http;

import java.util.Optional;

/**
 * Before class
 *
 * @author Ganeshwara Herawan Hananda
 */
public class Before {
    public static Before allow() {
        return new Before();
    }

    public static Before deny(Response response) {
        return new Before(response);
    }

    private Before(Response responseIfDenied) {
        this.responseIfDenied = Optional.of(responseIfDenied);
    }

    public Optional<Response> getResponseIfDenied() {
        return responseIfDenied;
    }

    private Before() {
        this.responseIfDenied = Optional.empty();
    }

    private final Optional<Response> responseIfDenied;
}
