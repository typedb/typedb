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

package io.mindmaps.migration.template;

public class Variable {

    private String variable;
    private boolean graqlVariable = false;
    private boolean comboVariable = false;

    public Variable(String var){
        this.variable = var;
        if(var.contains("%$")){
            this.comboVariable = true;
        } else if(var.contains("$")){
            this.graqlVariable = true;
        }
    }

    public String getVariable() {
        return variable;
    }

    public boolean isGraqlVariable() {
        return graqlVariable;
    }

    public boolean isComboVariable(){
        return comboVariable;
    }

    public String variable(){
        return variable;
    }

    public Variable cleaned(){
        if(isGraqlVariable()){
            return new Variable(variable.replace("$", ""));
        }
        else {
            return new Variable(variable.replace("%", ""));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Variable variable1 = (Variable) o;

        return graqlVariable == variable1.graqlVariable && variable.equals(variable1.variable);

    }

    @Override
    public int hashCode() {
        int result = variable.hashCode();
        result = 31 * result + (graqlVariable ? 1 : 0);
        return result;
    }
}
