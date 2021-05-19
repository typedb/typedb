/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.graph.structure;

import com.vaticle.typedb.core.common.collection.Bytes;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.iid.StructureIID;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;

public interface RuleStructure extends Bytes.ByteComparable<RuleStructure> {

    StructureIID.Rule iid();

    void iid(StructureIID.Rule iid);

    String label();

    void label(String label);

    Encoding.Status status();

    void setModified();

    boolean isModified();

    void delete();

    boolean isDeleted();

    Conjunction<? extends Pattern> when();

    ThingVariable<?> then();

    /**
     * Commits this {@code RuleStructure} to be persisted onto storage.
     */
    void commit();

    void indexConcludesVertex(Label label);

    void unindexConcludesVertex(Label label);

    void indexConcludesEdgeTo(Label type);

    void unindexConcludesEdgeTo(Label type);
}

