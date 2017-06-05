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

package ai.grakn.generator;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.manager.StandaloneTaskManager;
import ai.grakn.engine.tasks.manager.singlequeue.SingleQueueTaskManager;
import ai.grakn.engine.util.EngineID;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * TaskManagers
 * 
 * @author alex
 */
public class TaskManagers extends Generator<TaskManager> {

    @SuppressWarnings("unchecked")
    private Class<? extends TaskManager>[] taskManagerClasses = new Class[]{
            StandaloneTaskManager.class, SingleQueueTaskManager.class
    };

    private static Map<Class<? extends TaskManager>, TaskManager> taskManagers = new HashMap<>();

    public static void closeAndClear(){
        taskManagers.values().forEach(TaskManager::close);
        taskManagers.clear();
    }

    public TaskManagers(){
        super(TaskManager.class);
    }

    @Override
    public TaskManager generate(SourceOfRandomness random, GenerationStatus status) {
        Class<? extends TaskManager> taskManagerToReturn = random.choose(taskManagerClasses);

        GraknEngineConfig config = GraknEngineConfig.create();

        if(!taskManagers.containsKey(taskManagerToReturn)){
            try {
                Constructor<? extends TaskManager> constructor =
                        taskManagerToReturn.getConstructor(EngineID.class, GraknEngineConfig.class);
                taskManagers.put(taskManagerToReturn, constructor.newInstance(EngineID.me(), config));
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }

        return taskManagers.get(taskManagerToReturn);
    }
}
