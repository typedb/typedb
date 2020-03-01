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

package grakn.core.graql.reasoner.query;

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.server.Transaction;
import grakn.verification.tools.operator.TypeContext;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TransactionContext implements TypeContext {
    private final Transaction tx;
    private final List<ConceptId> ids;
    private final Random rand = new Random();

    TransactionContext(Transaction tx){
        this.tx = tx;
        this.ids = tx.getMetaConcept().instances()
                .map(Thing::asThing)
                .map(Concept::id)
                .collect(Collectors.toList());
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

    @Override
    public String instanceId() {
        return ids.get(rand.nextInt(ids.size())).getValue();
    }
}
