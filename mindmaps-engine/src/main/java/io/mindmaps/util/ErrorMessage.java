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

package io.mindmaps.util;

public enum ErrorMessage {
    NO_CONFIG_FILE("Cannot find config file [%s]"),
    PARSING_EXCEPTION("Parsing exception caused by [%s]"),
    CONCEPT_ID_NOT_FOUND("ID [%s] not found in the graph."),
    FAILED_TRANSACTION("Could not commit to graph after %s retries"),
    FAILED_VALIDATION("Caught exception during validation: %s"),
    NO_PARAMETER_PROVIDED("Required Parameter [%s] is missing for this [%s] operation"),

    //Post processing Errors
    CONCEPT_POSTPROCESSING("Concept [%s] of type [%s] is doe not have any post-processing steps"),
    POSTPROCESSING_ERROR("Unexpected error during %s fix due to [%s]"),
    UNABLE_TO_ANALYSE_CONCEPT("Unable to analyse concept [%s] due to error [%s]"),
    BACK_OFF_RETRY("Unexpected failure performing backoff and retry of [%s]S"),
    //Distributed loading Errors
    ERROR_IN_DISTRIBUTED_TRANSACTION("Error while sending transaction to host: [%s]. Code: [%s] Message:[%s] \n Transaction string: [%s] ");

    private final String message;

    ErrorMessage(String message) {
        this.message = message;
    }

    public String getMessage(Object... args) {
        return String.format(message, args);
    }
}
