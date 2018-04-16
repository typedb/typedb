/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.kbs;

/*-
 * #%L
 * grakn-test-tools
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.GraknTx;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.SampleKBLoader;

/**
 *
 * @author Sheldon
 *
 */
public class SNBKB extends TestKB {

    public static SampleKBContext context() {
        return new SNBKB().makeContext();
    }

    @Override
    protected void buildSchema(GraknTx tx) {
        SampleKBLoader.loadFromFile(tx, "ldbc-snb-schema.gql");
        SampleKBLoader.loadFromFile(tx, "ldbc-snb-product-schema.gql");
    }

    @Override
    protected void buildRules(GraknTx tx) {
        SampleKBLoader.loadFromFile(tx, "ldbc-snb-rules.gql");
    }

    @Override
    protected void buildInstances(GraknTx tx) {
        SampleKBLoader.loadFromFile(tx, "ldbc-snb-data.gql");
    }
}
