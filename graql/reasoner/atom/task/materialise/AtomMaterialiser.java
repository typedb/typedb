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
package grakn.core.graql.reasoner.atom.task.materialise;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;

import java.util.stream.Stream;

public interface AtomMaterialiser<T extends Atom> {
    
    /**
     * Materialises the provided atom - does an insert of the corresponding pattern.
     * Exhibits PUT behaviour - if things are already present, nothing is inserted.
     *
     * @return materialised answer to this atom
     */
    Stream<ConceptMap> materialise(T toMaterialise, ReasoningContext ctx);
}
