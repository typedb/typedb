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

package ai.grakn.test;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.backgroundtasks.TaskManager;
import org.junit.rules.ExternalResource;

import static ai.grakn.test.GraknTestEnv.hideLogs;
import static ai.grakn.test.GraknTestEnv.randomKeyspace;
import static ai.grakn.test.GraknTestEnv.startEngine;
import static ai.grakn.test.GraknTestEnv.startKafka;
import static ai.grakn.test.GraknTestEnv.stopEngine;
import static ai.grakn.test.GraknTestEnv.stopKafka;

/**
 * <p>
 * Start the Grakn Engine server before each test class and stop after.
 * </p>
 *
 * @author alexandraorth
 */
public class EngineContext extends ExternalResource {

    private final boolean startKafka;
    private final boolean startDistributedEngine;
    private final boolean startInMemoryEngine;

    protected EngineContext(boolean startKafka, boolean startDistributedEngine, boolean startInMemoryEngine){
        this.startDistributedEngine = startDistributedEngine;
        this.startInMemoryEngine = startInMemoryEngine;
        this.startKafka = startKafka;
    }

    public static EngineContext startKafkaServer(){
        return new EngineContext(true, false, false);
    }

    public static EngineContext startDistributedServer(){
        return new EngineContext(true, true, false);
    }

    public static EngineContext startInMemoryServer(){
        return new EngineContext(false, false, true);
    }

    public TaskManager getTaskManager(){
        return GraknEngineServer.getTaskManager();
    }

    public GraknGraphFactory factoryWithNewKeyspace() {
        return Grakn.factory(Grakn.DEFAULT_URI, randomKeyspace());
    }

    @Override
    public void before() throws Throwable {
        hideLogs();

        if(startKafka){
            startKafka();
        }

        if (startDistributedEngine){
            startEngine(true);
        }

        if (startInMemoryEngine){
            startEngine(false);
        }
    }

    @Override
    public void after() {
        try {
            if(startDistributedEngine | startInMemoryEngine){
                stopEngine();
            }

            if(startKafka){
                stopKafka();
            }
        } catch (Exception e){
            throw new RuntimeException("Could not shut down ", e);
        }
    }
}
