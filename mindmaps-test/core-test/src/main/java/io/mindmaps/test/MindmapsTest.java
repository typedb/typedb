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

package io.mindmaps.test;

public class MindmapsTest {

    private MindmapsTest() {}

    public static final String TEST_IMPLEMENTATION = "io.mindmaps.test.MindmapsEngineTest";

    private static <F extends AbstractMindmapsEngineTest> F loadImplementation(String className) {
        try {
            @SuppressWarnings("unchecked")
            Class<F> cl = (Class<F>)Class.forName(className);
            return cl.getConstructor().newInstance();
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static AbstractMindmapsEngineTest get() {
        return loadImplementation(TEST_IMPLEMENTATION);
    }
}
