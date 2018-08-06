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

package ai.grakn.engine.attribute.uniqueness.queue;

import com.google.common.base.MoreObjects;

import java.util.List;

/**
 * TODO
 * @author Ganeshwara Herawan Hananda
 */
public class Attributes {
    List<Attribute> attributes;

    Attributes(List<Attribute> attributes) {
        this.attributes = attributes;
    }

    // TODO
    public List<Attribute> attributes() {
        return attributes;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("attributes", attributes)
                .toString();
    }
}
