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

package hypergraph.common.exception;

import java.util.List;

public class HypergraphException extends RuntimeException {

    public HypergraphException(String error) {
        super(error);
    }

    public HypergraphException(Error error) {
        super(error.toString());
        assert !getMessage().contains("%s");
    }

    public HypergraphException(Exception e) {
        super(e);
    }

    public HypergraphException(List<HypergraphException> exceptions) {
        super(getMessages(exceptions));
    }

    private static String getMessages(List<HypergraphException> exceptions) {
        StringBuilder messages = new StringBuilder();
        for (HypergraphException exception : exceptions) {
            messages.append(exception.getMessage()).append("\n");
        }
        return messages.toString();
    }
}
