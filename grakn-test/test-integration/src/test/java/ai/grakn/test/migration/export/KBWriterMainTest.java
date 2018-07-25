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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.test.migration.export;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.migration.export.Main;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.util.SampleKBLoader;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class KBWriterMainTest {

    private static Keyspace keyspace;

    @ClassRule
    public static final EngineContext engine = EngineContext.create();

    @Rule
    public final SystemOutRule sysOut = new SystemOutRule().enableLog();

    @Rule
    public final SystemErrRule sysErr = new SystemErrRule().enableLog();

    @BeforeClass
    public static void loadMovieKB() {
        keyspace = SampleKBLoader.randomKeyspace();
        try(GraknTx tx = new Grakn(engine.grpcUri()).session(keyspace).transaction(GraknTxType.WRITE)){
            MovieKB.get().accept(tx);
            tx.commit();
        }
    }

    @Test
    public void exportCalledWithSchemaFlag_DataPrintedToSystemOut(){
        run("export", "-u", engine.uri().toString(), "-schema", "-keyspace", keyspace.getValue());

        assertThat(sysOut.getLog(), containsString("sub entity"));
    }

    @Test
    public void exportCalledWithDataFlag_DataPrintedToSystemOutTest(){
        run("export", "-u", engine.uri().toString(), "-data", "-keyspace", keyspace.getValue());

        assertThat(sysOut.getLog(), containsString("isa movie"));
    }
    
    @Test
    public void exportCalledWithNoArgs_HelpMessagePrintedToSystemOut(){
        run("export", "schema");

        assertThat(sysOut.getLog(), containsString("usage: graql migrate"));
    }

    @Test
    public void exportCalledWithHelpFlag_HelpMessagePrintedToSystemOut(){
        run("export", "-h");

        assertThat(sysOut.getLog(), containsString("usage: graql migrate"));
    }

    @Test
    public void exportCalledWithIncorrectURI_ErrorIsPrintedToSystemErr(){
        run("export", "-u", engine.uri().toString().substring(1), "-data", "-keyspace", keyspace.getValue());

        assertThat(sysErr.getLog(), containsString("Could not connect to Grakn Engine. Have you run 'grakn server start'?"));
    }

    private void run(String... args){
        Main.main(args);
    }
}
