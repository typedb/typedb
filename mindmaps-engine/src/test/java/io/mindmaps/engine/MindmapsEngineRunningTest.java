/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.engine;

import com.jayway.restassured.RestAssured;
import io.mindmaps.engine.util.ConfigProperties;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class MindmapsEngineRunningTest {

    @Before
    public void setCorrectProperties(){
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, ConfigProperties.TEST_CONFIG_FILE);
        Properties prop = ConfigProperties.getInstance().getProperties();
        RestAssured.baseURI = "http://" + prop.getProperty("server.host") + ":" + prop.getProperty("server.port");
    }

    @Test
    public void mindmapsEngineRunning() throws InterruptedException {
        MindmapsEngineServer.start();

        Thread.sleep(5000);

        boolean running = MindmapsEngineServer.isRunning();
        assertTrue(running);

        MindmapsEngineServer.stop();
        Thread.sleep(5000);
    }

    @Test
    public void mindmapsEngineNotRunning(){
        boolean running = MindmapsEngineServer.isRunning();
        assertFalse(running);
    }
}
