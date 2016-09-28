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

package io.mindmaps.migration.json;

import mjson.Json;

import java.net.URL;

class Util {

    public static Json readJson(String name) {
        URL dataUrl = Util.class.getClassLoader().getResource(name);
        assert dataUrl != null;
        return Json.read(dataUrl);
    }

    public static Json.Schema readSchema(String name) {
        return Json.schema(readJson(name + "/schema.json"));
    }
}
