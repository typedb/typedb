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

package grakn.core.traversal.procedure;

import grakn.core.traversal.Identifier;

import java.util.HashSet;
import java.util.Set;

class ProcedureVertex {

    private final Identifier identifier;
    private final Procedure procedure;
    private final Set<ProcedureEdge> outgoing;
    private final Set<ProcedureEdge> incoming;

    ProcedureVertex(Identifier identifier, Procedure procedure) {
        this.identifier = identifier;
        this.procedure = procedure;
        this.outgoing = new HashSet<>();
        this.incoming = new HashSet<>();
    }

    void out(ProcedureEdge edge) {
        // TODO
    }

    void in(ProcedureEdge edge) {
        // TODO
    }

    Set<ProcedureEdge> outs() {
        return outgoing;
    }

    Set<ProcedureEdge> ins() {
        return incoming;
    }

    Identifier identifier() {
        return identifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProcedureVertex that = (ProcedureVertex) o;
        return this.identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return identifier.hashCode();
    }
}
