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
import ai.grakn.util.SampleKBLoader;

import java.util.function.Consumer;

/**
 *
 * @author Kasper Piskorski
 *
 */
public abstract class AbstractPathKB extends TestKB {
    private final Label key;
    private final String gqlFile;
    private final int n;
    private final int m;

    AbstractPathKB(String gqlFile, Label key, int n, int m){
        this.gqlFile = gqlFile;
        this.key = key;
        this.n = n;
        this.m = m;
    }

    @Override
    public Consumer<GraknTx> build(){
        return (GraknTx tx) -> {
            SampleKBLoader.loadFromFile(tx, gqlFile);
            buildExtensionalDB(tx, n, m);
        };
    }

    Label getKey(){ return key;}

    abstract protected void buildExtensionalDB(GraknTx tx, int n, int children);
}
