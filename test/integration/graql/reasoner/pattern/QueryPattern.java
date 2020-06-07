/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.reasoner.pattern;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class QueryPattern {

    @Override
    public String toString(){
        return String.join("\n", patterns());
    }

    public abstract List<String> patterns();

    public abstract int size();

    public int[][] exactMatrix() { return identity(size()); }

    public abstract int[][] structuralMatrix();

    public abstract int[][] ruleMatrix();

    public int[][] subsumptionMatrix(){return zeroMatrix(size(), size());}

    public static int[][] identity(int N){
        int[][] matrix = new int[N][N];
        for(int i = 0; i < N ; i++) {
            for (int j = 0; j < N; j++) {
                if (i == j) matrix[i][j] = 1;
                else matrix[i][j] = 0;
            }
        }
        return matrix;
    }

    public static int[][] zeroMatrix(int N, int M){
        int[][] matrix = new int[N][M];
        for(int i = 0; i < N ; i++) {
            for (int j = 0; j < M; j++) {
                matrix[i][j] = 0;
            }
        }
        return matrix;
    }

    public static <T> List<T> subList(List<T> list, Collection<Integer> elements){
        List<T> subList = new ArrayList<>();
        elements.forEach(el -> subList.add(list.get(el)));
        return subList;
    }

    public static <T> List<T> subListExcluding(List<T> list, Collection<Integer> toExclude){
        List<T> subList = new ArrayList<>(list);
        toExclude.forEach(el -> subList.remove(list.get(el)));
        return subList;
    }

    public static <T> List<T> subListExcludingElements(List<T> list, Collection<T> toExclude){
        List<T> subList = new ArrayList<>(list);
        toExclude.forEach(subList::remove);
        return subList;
    }

}
