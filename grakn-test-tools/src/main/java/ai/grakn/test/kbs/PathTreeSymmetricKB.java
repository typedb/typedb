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

package ai.grakn.test.kbs;

import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.concept.Role;
import ai.grakn.test.rule.SampleKBContext;

/**
 *
 * @author Kasper Piskorski
 *
 */
public class PathTreeSymmetricKB extends PathTreeKB {

    private PathTreeSymmetricKB(int n, int m){
        super("path-test-symmetric.gql", Label.of("index"), n, m);
    }

    public static SampleKBContext context(int n, int m) {
        return new PathTreeSymmetricKB(n, m).makeContext();
    }

    @Override
    protected void buildExtensionalDB(GraknTx tx, int n, int children) {
        Role coordinate = tx.getRole("coordinate");
        buildTree(tx, coordinate, coordinate, n , children);
    }
}
