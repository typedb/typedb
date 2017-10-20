/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.kb.internal;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.UUID;

public class TxTestBase {
    protected GraknSession session;
    protected GraknTxAbstract<?> tx;
    private GraknTxAbstract<?> txBatch;
    //haha is here because the keyspace has to start with a letter
    private String keyspace = "haha" + UUID.randomUUID().toString().replaceAll("-", "a");

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();


    @Before
    public void setUpTx() {
        session = Grakn.session(Grakn.IN_MEMORY, keyspace);
        getTx(false);
    }

    @After
    public void closeSession() throws Exception {
        closeTxIfOpen(tx);
        closeTxIfOpen(txBatch);
        session.close();
    }

    protected GraknTxAbstract<?> tx(){
        return getTx(false);
    }

    protected GraknTxAbstract<?> batchTx(){
       return getTx(true);
    }

    private GraknTxAbstract<?> getTx(boolean isBatch){
        if(isBatch){
            if(newTxNeeded(txBatch)){
                closeTxIfOpen(tx);
                return txBatch = (GraknTxAbstract) Grakn.session(Grakn.IN_MEMORY, keyspace).open(GraknTxType.BATCH);
            } else {
                return txBatch;
            }
        } else {
            if(newTxNeeded(tx)){
                closeTxIfOpen(txBatch);
                return tx = (GraknTxAbstract) Grakn.session(Grakn.IN_MEMORY, keyspace).open(GraknTxType.WRITE);
            } else {
                return tx;
            }
        }
    }

    private boolean newTxNeeded(GraknTxAbstract<?> tx){
        return tx == null || tx.isClosed();
    }

    private void closeTxIfOpen(GraknTxAbstract<?> tx){
        if(tx != null && !tx.isClosed()) tx.close();
    }

}