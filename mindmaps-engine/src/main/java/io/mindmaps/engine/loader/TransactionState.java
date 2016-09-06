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

package io.mindmaps.engine.loader;

import mjson.Json;

/**
 * Class that enumerates the possible states of a transaction and
 * an exception associates with that state if it is an error.
 */
public class TransactionState{

    public enum State {
        QUEUED, LOADING, FINISHED, ERROR
    }

    private State currentState;
    private String exception;
    private final String STATE_FIELD = "state";
    private final String EXCEPTION_FIELD = "exception";


    public TransactionState(State state){
        currentState=state;
    }

    public void setException(String exceptionParam){
        exception=exceptionParam;
    }

    public void setState(State stateParam){ currentState=stateParam;}

    public State getState(){
        return currentState;
    }

    public String getException(){return exception;}


    @Override
    public String toString() {
        return Json.object()
                .set(STATE_FIELD, currentState.toString())
                .set(EXCEPTION_FIELD, exception)
                .toString();
    }
}
