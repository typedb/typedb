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

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.util.ConfigProperties;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.RESTUtil;
import io.mindmaps.visualiser.HALConcept;
import spark.Request;
import spark.Response;

import static spark.Spark.get;

public class VisualiserController {

    private String defaultGraphName;

    public VisualiserController() {

        defaultGraphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);

        get(RESTUtil.WebPath.CONCEPTS_BY_VALUE_URI, this::getConceptsByValue);

        get(RESTUtil.WebPath.CONCEPT_BY_ID_URI, this::getConceptById);

    }

    private String getConceptsByValue(Request req, Response res) {

        // TODO: Implement HAL builder for concepts retrieved by Value

        GraphFactory.getInstance().getGraph(defaultGraphName).newTransaction().getConceptsByValue(req.queryParams(RESTUtil.Request.VALUE_FIELD));
        return req.queryParams(RESTUtil.Request.VALUE_FIELD);
    }

    private String getConceptById(Request req, Response res) {

        MindmapsTransaction transaction = GraphFactory.getInstance().getGraph(defaultGraphName).newTransaction();

        Concept concept = transaction.getConcept(req.params(RESTUtil.Request.ID_PARAMETER));
        if (concept != null)
            return new HALConcept(concept).render();
        else {
            res.status(404);
            return ErrorMessage.CONCEPT_ID_NOT_FOUND.getMessage(req.params(RESTUtil.Request.ID_PARAMETER));
        }
    }

}
