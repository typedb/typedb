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

package io.mindmaps.migration.base;

import io.mindmaps.graql.Var;

import java.io.File;
import java.io.Reader;
import java.util.Collection;
import java.util.stream.Stream;

public interface Migrator {

    /**
     * Migrate all the data in the given file based on the given template.
     * @param template parametrized graql insert query
     * @param file file containing data to be migrated
     */
    public  Stream<Collection<Var>> migrate(String template, File file);

    /**
     * Migrate all the data in the given file based on the given template.
     * @param template parametrized graql insert query
     * @param reader reader over the data to be migrated
     */
    public Stream<Collection<Var>> migrate(String template, Reader reader);
}
