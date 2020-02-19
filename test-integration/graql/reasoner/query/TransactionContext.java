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
 *
 */

package grakn.core.graql.reasoner.query;

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.server.Transaction;
import grakn.theory.tools.operator.TypeContext;
import java.util.stream.Stream;

class TransactionContext implements TypeContext {
    private final Transaction tx;

    TransactionContext(Transaction tx){
        this.tx = tx;
    }

    @Override
    public boolean isMetaType(String label) {
        return Schema.MetaSchema.isMetaLabel(Label.of(label));
    }

    @Override
    public String sup(String label) {
        SchemaConcept type = tx.getSchemaConcept(Label.of(label));
        if (type == null) return null;
        SchemaConcept sup = type.sup();
        if (sup == null) return null;
        return sup.label().getValue();
    }

    @Override
    public Stream<String> sups(String label) {
        SchemaConcept type = tx.getSchemaConcept(Label.of(label));
        if (type == null) return Stream.empty();
        return tx.sups(type).map(t -> t.label().getValue());
    }

    @Override
    public Stream<String> subs(String label) {
        SchemaConcept type = tx.getSchemaConcept(Label.of(label));
        if (type == null) return Stream.empty();
        return type.subs().map(t -> t.label().getValue());
    }
}
