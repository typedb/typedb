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

package io.mindmaps.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * A set of structures in memory and a logger to reflect the state of the graph loader.
 *
 */

public class StateVariables {
    private final Map<UUID,State> loaderState = new ConcurrentHashMap<>();
    private final Map<UUID,List<String>> statusBuffer = new ConcurrentHashMap<>();
    private final Map<UUID,List<String>> errorBuffer = new ConcurrentHashMap<>();
    private final Logger LOG = LoggerFactory.getLogger(StateVariables.class);

    public StateVariables() {

    }

    // Adders
    public void addTransaction (UUID transactionID) {
        if (loaderState.containsKey(transactionID)) {
            throw new RuntimeException("Attempted to add a job to the queue using an existing transactionID.");
        }
        loaderState.put(transactionID, State.QUEUED);
        statusBuffer.put(transactionID, new ArrayList<>());
        errorBuffer.put(transactionID, new ArrayList<>());
        String message = String.format("Added new job with transactionID %s to the queue",transactionID.toString());
        addStatus(transactionID, message);
    }

    public void addStatus(UUID transactionID, String statusMessage) {
        if (!statusBuffer.containsKey(transactionID)) {
            throw new RuntimeException("Attempted to add a status message to a non-existent job.");
        }
        statusBuffer.get(transactionID).add(statusMessage);
        LOG.info(transactionID.toString() + ": " + statusMessage);
    }

    public void addError(UUID transactionID, String errorMessage) {
        if (!statusBuffer.containsKey(transactionID)) {
            throw new RuntimeException("Attempted to add an error message to a non-existent job.");
        }
        statusBuffer.get(transactionID).add(errorMessage);
        errorBuffer.get(transactionID).add(errorMessage);
        LOG.error(transactionID.toString() + ": " + errorMessage);
    }

    // Getters
    public Map<UUID,State> getStates () {
        return loaderState;
    }

    public State getState (UUID transactionID) {
        return loaderState.get(transactionID);
    }

    public Map<UUID,List<String>> getStatus() {
        // send back a deep copy for safety
        Map<UUID,List<String>> newBuffer = new HashMap<>();
        statusBuffer.keySet().forEach(key -> newBuffer.put(key,new ArrayList<>()));
        newBuffer.forEach((key,value) -> statusBuffer.get(key).forEach(listItem -> value.add(listItem)));
        return newBuffer;
    }

    public List<String> getStatus(UUID transactionID) {
        return new ArrayList<>(statusBuffer.get(transactionID));
    }

    public List<String> getErrors(UUID transactionID) {
        return new ArrayList<>(errorBuffer.get(transactionID));
    }

    // Putters
    public void putState (UUID transactionID, State newState) {
        if (!loaderState.containsKey(transactionID)) {
            throw new RuntimeException("Requested change of state for job that does not exist.");
        }
        loaderState.put(transactionID, newState);
        String message = String.format("Changed state of job with transactionID %s to %s",
                transactionID.toString(), newState.toString());
        addStatus(transactionID, message);
    }
}
