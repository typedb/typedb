package io.mindmaps.loader;

import org.json.JSONObject;

public class TransactionState{

    public enum State {
        QUEUED, LOADING, FINISHED, ERROR, CANCELLED
    }

    private State currentState;
    private String exception;

    public TransactionState(State state){
        currentState=state;
    }

    public TransactionState(String json){
        JSONObject obj = new JSONObject(json);
        if(obj.has("state")){
            String state = obj.getString("state");

            if(state.equals(State.QUEUED.name())) {
                this.currentState = State.QUEUED;
            } else if(state.equals(State.FINISHED.name())) {
                this.currentState = State.FINISHED;
            } else if(state.equals(State.LOADING.name())) {
                this.currentState = State.LOADING;
            } else if(state.equals(State.ERROR.name())) {
                this.currentState = State.ERROR;
            } else if(state.equals(State.CANCELLED.name())) {
                this.currentState = State.CANCELLED;
            }
        }

        if(obj.has("exception")) {
            this.exception = obj.getString("exception");
        }
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
        return "{ \"state\":\"" + currentState + "\", " +
                " \"exception\":\"" + exception + "\"}";
    }
}
