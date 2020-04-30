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
 */

package grakn.core.graql.reasoner.graph;

import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;

@SuppressWarnings("CheckReturnValue")
public class PathTreeSymmetricGraph extends PathTreeGraph {

    public PathTreeSymmetricGraph(Session session){
        super(session, "pathTest-symmetric.gql");
    }

    @Override
    protected void buildExtensionalDB(int n, int children, Transaction tx) {
        buildTree("coordinate", "coordinate", n , children, tx);
    }
}
