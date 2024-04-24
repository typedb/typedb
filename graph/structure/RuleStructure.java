/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.graph.structure;

import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.StructureIID;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.statement.ThingStatement;

public interface RuleStructure {

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

    ThingStatement<?> then();

    /**
     * Commits this {@code RuleStructure} to be persisted onto storage.
     */
    void commit();

    void indexConcludesVertex(Label label);

    void unindexConcludesVertex(Label label);

    void indexConcludesEdgeTo(Label type);

    void unindexConcludesEdgeTo(Label type);
}

