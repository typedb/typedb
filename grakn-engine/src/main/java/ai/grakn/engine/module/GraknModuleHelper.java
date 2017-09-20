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
 *
 */

package ai.grakn.engine.module;

import ai.grakn.graknmodule.GraknModule;
import ai.grakn.graknmodule.http.Before;
import ai.grakn.graknmodule.http.BeforeHttpEndpoint;
import ai.grakn.graknmodule.http.HttpEndpoint;
import ai.grakn.graknmodule.http.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Filter;
import spark.Request;
import spark.Route;
import spark.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static spark.Spark.halt;

/**
 * Grakn Module helper
 *
 * @author Ganeshwara Herawan Hananda
 */
public class GraknModuleHelper {
    private static final Logger LOG = LoggerFactory.getLogger(GraknModuleHelper.class);

    public static Function<HttpEndpoint, Route> convertEndpointToSparkRoute = (graknModule ->
        ((request, response) -> {
            ai.grakn.graknmodule.http.Request graknModuleRequest =
                new ai.grakn.graknmodule.http.Request(getHeaders(request), request.params(), request.body());

            ai.grakn.graknmodule.http.Response graknModuleResponse = graknModule.getRequestHandler(graknModuleRequest);

            response.status(graknModuleResponse.getStatusCode());
            return graknModuleResponse.getBody();
        })
    );

    public static Function<BeforeHttpEndpoint, Filter> convertBeforeToSparkFilter = (graknModule ->
        ((request, response) -> {
            ai.grakn.graknmodule.http.Request graknModuleRequest =
                new ai.grakn.graknmodule.http.Request(getHeaders(request), request.params(), request.body());

            Before before = graknModule.getBeforeHttpEndpoint(graknModuleRequest);

            if (before.getResponseIfDenied().isPresent()) {
                Response graknModuleResponse = before.getResponseIfDenied().get();
                halt(graknModuleResponse.getStatusCode(), graknModuleResponse.getBody());
            }

            // else let the request pass
        })
    );

    public static void registerGraknModuleBeforeHttpEndpoints(Service spark, List<GraknModule> graknModules) {
        for (GraknModule module : graknModules) {
            for (BeforeHttpEndpoint endpoint: module.getBeforeHttpEndpoints())
                spark.before(endpoint.getUrlPattern(), convertBeforeToSparkFilter.apply(endpoint));
        }
    }

    public static void registerGraknModuleHttpEndpoints(Service spark, List<GraknModule> graknModules) {
        for (GraknModule module : graknModules) {
            for (HttpEndpoint endpoint: module.getHttpEndpoints())
                switch (endpoint.getHttpMethod()) {
                    case GET:
                        spark.get(endpoint.getEndpoint(), convertEndpointToSparkRoute.apply(endpoint));
                        break;
                    case POST:
                        spark.post(endpoint.getEndpoint(), convertEndpointToSparkRoute.apply(endpoint));
                        break;
                    case PUT:
                        spark.put(endpoint.getEndpoint(), convertEndpointToSparkRoute.apply(endpoint));
                        break;
                    case DELETE:
                        spark.delete(endpoint.getEndpoint(), convertEndpointToSparkRoute.apply(endpoint));
                        break;
                }
        }
    }

    public static Stream<Path> listFolders(Path directory) {
        try {
            return Files.list(directory)
                .filter(e -> e.toFile().isDirectory());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Stream<Path> listJarFiles(Path directory) {
        try {
            return Files.list(directory)
                .filter(e -> e.toFile().isFile())
                .filter(e -> e.getFileName().toString().endsWith(".jar"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, String> getHeaders(Request request) {
        Stream<Pair<String, String>> headersAsStream = request.headers().stream().map(e -> Pair.of(e, request.headers(e)));

        request.headers().forEach(e -> LOG.info("hdrz().stream().foreach -- " + e + "-" + request.headers(e)));

        Map<String, String> headersAsMap = new HashMap<>();
        headersAsStream.forEach(e -> headersAsMap.put(e.getKey(), e.getValue()));
        LOG.info("headersAsMap size = " + headersAsMap.size());
        return headersAsMap;
    }
}
