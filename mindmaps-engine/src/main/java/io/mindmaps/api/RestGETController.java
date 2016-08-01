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

package io.mindmaps.api;

import io.mindmaps.core.implementation.MindmapsTransactionImpl;
import io.mindmaps.core.model.Concept;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.api.parser.QueryParser;
import org.json.JSONArray;
import org.json.JSONObject;

import static spark.Spark.get;

public class RestGETController {

    MindmapsTransactionImpl graphTransaction;

    public RestGETController() {

        graphTransaction = GraphFactory.getInstance().buildMindmapsGraph();

        get("/", (req, res) -> "こんにちは from Mindmaps Engine!");

        get("/select", (req, res) -> {
            QueryParser parser = QueryParser.create(graphTransaction);
            JSONArray response = new JSONArray();
            parser.parseMatchQuery(req.queryParams("query")).getMatchQuery()
                    .forEach(result ->
                                    result.keySet()
                                            .iterator()
                                            .forEachRemaining(variable -> {
                                                JSONObject jsonVariableObj = new JSONObject();
                                                jsonVariableObj.put(variable, buildJSONObject(result.get(variable)));
                                                response.put(jsonVariableObj);
                                            })
                    );

            return response.toString();
        });

    }


    private JSONObject buildJSONObject(Concept currentConcept) {

        JSONObject jsonConcept = new JSONObject();

        jsonConcept.put("id", currentConcept.getId());

        Object value = currentConcept.getValue();
        jsonConcept.put("value", (value != null) ? value.toString() : "");

        String type = currentConcept.type().getId();
        jsonConcept.put("type", (type != null) ? type : "");

        String baseType = currentConcept.type().type().getId();
        jsonConcept.put("baseType", (baseType != null) ? baseType : "");


        return jsonConcept;
    }
}
