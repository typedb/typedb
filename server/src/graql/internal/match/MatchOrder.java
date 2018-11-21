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

package grakn.core.graql.internal.match;

import grakn.core.graql.query.Match;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.server.session.TransactionImpl;

import java.util.stream.Stream;

/**
 * "Order" modify that orders the underlying {@link Match}
 *
 */
class MatchOrder extends MatchModifier {

    private final Ordering order;

    MatchOrder(AbstractMatch inner, Ordering order) {
        super(inner);
        this.order = order;
    }

    @Override
    public Stream<ConceptMap> stream(TransactionImpl<?> tx) {
        return order.orderStream(inner.stream(tx));
    }

    @Override
    protected String modifierString() {
        return " " + order.toString() + ";";
    }
}
