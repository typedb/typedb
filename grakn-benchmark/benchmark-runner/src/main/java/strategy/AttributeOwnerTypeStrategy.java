/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package strategy;

import ai.grakn.concept.Type;
import pick.StreamProviderInterface;

/**
 * @param <T>
 */
// TODO implement a base interface of TypeStrategyInterface?
public class AttributeOwnerTypeStrategy<T> implements HasPicker {
    private final Type type;
    private final String typeLabel;
    private StreamProviderInterface<T> picker;

    public AttributeOwnerTypeStrategy(Type type, StreamProviderInterface<T> picker) {
        this.type = type;
        this.picker = picker;
        typeLabel = this.type.label().getValue();
    }

    @Override
    public StreamProviderInterface<T> getPicker() {
        return this.picker;
    }

    public String getTypeLabel() {
        return this.typeLabel;
    }
}