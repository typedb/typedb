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

import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.implementation.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.QueryBuilder;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.loader.BlockingLoader;
import io.mindmaps.util.ConfigProperties;
import io.mindmaps.util.RESTUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static spark.Spark.post;

/**
 * Class that provides methods to import ontologies and data from a Graql file to a graph.
 */

@Path("/import")
@Api(value = "/import", description = "Endpoints to import data and ontologies from Graqlfiles to a graph.")
@Produces("text/plain")

public class ImportController {

    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(ImportController.class);


    private String graphName;

    //TODO: Use redis for caching LRU
    private Map<String, String> entitiesMap;
    private ArrayList<Var> relationshipsList;

    private BlockingLoader loader;


    public ImportController() {
        new ImportController(ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY));
    }


    public ImportController(String graphNameInit) {

        post(RESTUtil.WebPath.IMPORT_DATA_URI, this::importDataREST);
        post(RESTUtil.WebPath.IMPORT_ONTOLOGY_URI, this::importOntologyREST);

        entitiesMap = new ConcurrentHashMap<>();
        relationshipsList = new ArrayList<>();
        graphName = graphNameInit;
        loader = new BlockingLoader(graphName);

    }


    @POST
    @Path("/data")
    @ApiOperation(
            value = "Import data from a Graql file. It performs batch loading.",
            notes = "This is a separate import from ontology, since a batch loading is performed to optimise the loading speed. ",
            response = String.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "path", value = "File path on the server.", required = true, dataType = "string", paramType = "body"),

    })
    private String importDataREST(Request req, Response res) {
        try {
            String filePath = new JSONObject(req.body()).getString(RESTUtil.Request.PATH_FIELD);
            importDataFromFile(filePath);
        } catch (JSONException j) {
            res.status(400);
            return j.getMessage();
        } catch (Exception e) {
            res.status(500);
            return e.getMessage();
        }
        return "";
    }

    @POST
    @Path("/ontology")
    @ApiOperation(
            value = "Import ontology from a Graql file. It does not perform any batching.",
            notes = "This is a separate import from data, since a batch loading is not performed in this case. The ontology must be loaded in one single transaction. ",
            response = String.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "path", value = "File path on the server.", required = true, dataType = "string", paramType = "body"),

    })
    private String importOntologyREST(Request req, Response res) {
        try {
            JSONObject bodyObject = new JSONObject(req.body());
            importOntologyFromFile(bodyObject.get(RESTUtil.Request.PATH_FIELD).toString());
        } catch (JSONException j) {
            res.status(400);
            return j.getMessage();
        } catch (Exception e) {
            e.printStackTrace();
            res.status(500);
            return e.getMessage();
        }
        return "";
    }

    void importDataFromFile(String dataFile) throws IOException {
        QueryParser.create().parsePatternsStream(new FileInputStream(dataFile)).forEach(pattern -> consumeEntity(pattern.admin().asVar()));
        loader.waitToFinish();
        QueryParser.create().parsePatternsStream(new FileInputStream(dataFile)).forEach(pattern -> consumeRelation(pattern.admin().asVar()));
        loader.waitToFinish();
    }

    private void consumeEntity(Var var) {
        if (!entitiesMap.containsKey(var.admin().getName()) && !var.admin().isRelation() && var.admin().getType().isPresent()) {
            if (var.admin().isUserDefinedName()) {
                // Some variable might not have an explicit ID defined, in that case we generate explicitly one and we save it into our cache
                // so that we can refer to it.
                String varId = (var.admin().getId().isPresent()) ? var.admin().getId().get() : UUID.randomUUID().toString();
                entitiesMap.put(var.admin().getName(), varId);
                // We force the ID of the current var to be the one computed by this controller.
                loader.addToQueue(var.admin().id(varId));
            } else {
                loader.addToQueue(var);
            }
        }
    }

    private void consumeRelation(Var var) {
        boolean ready = false;
        if (var.admin().isRelation()) {
            ready = true;
            //If one of the role players is defined using a variable name and the variable name is not in our cache we cannot insert the relation.
            for (Var.Casting x : var.admin().getCastings()) {
                if (x.getRolePlayer().admin().isUserDefinedName()) {
                    if (entitiesMap.containsKey(x.getRolePlayer().getName()))
                        x.getRolePlayer().id(entitiesMap.get(x.getRolePlayer().getName()));
                    else ready = false;
                }
            }
        }
        if (ready) loader.addToQueue(var);
    }

    void importOntologyFromFile(String ontologyFile) throws IOException, MindmapsValidationException {

        MindmapsTransaction transaction = GraphFactory.getInstance().getGraph(graphName).newTransaction();
        List<Var> ontologyBatch = new ArrayList<>();

        LOG.info("Loading new ontology .. ");

        QueryParser.create().parsePatternsStream(new FileInputStream(ontologyFile)).map(x -> x.admin().asVar()).forEach(ontologyBatch::add);
        QueryBuilder.build(transaction).insert(ontologyBatch).execute();
        transaction.commit();

        LOG.info("Ontology loaded. ");

    }
}
