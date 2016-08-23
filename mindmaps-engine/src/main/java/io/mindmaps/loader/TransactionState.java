package io.mindmaps.loader;

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
                .set(STATE_FIELD, currentState)
                .set(EXCEPTION_FIELD, exception)
                .toString();
    }
}
