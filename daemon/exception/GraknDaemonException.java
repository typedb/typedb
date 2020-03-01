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

package grakn.core.daemon.exception;

import grakn.core.common.exception.GraknException;

/**
 * Grakn Daemon Exception
 */
public class GraknDaemonException extends GraknException {

    public GraknDaemonException(String message) {
        super(message);
    }

    public GraknDaemonException(String message, Exception e) {
        super(message, e);
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }
}
