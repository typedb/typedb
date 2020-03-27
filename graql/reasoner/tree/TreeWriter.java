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
 *
 */

package grakn.core.graql.reasoner.tree;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class TreeWriter {

    private Path filePath;

    TreeWriter(Path filePath){
        this.filePath = filePath;
    }

    private void writeNode(Node node, Map<Node, Integer> ids, BufferedWriter writer) throws IOException {
        Integer nodeId = ids.get(node);
        writer.write(String.valueOf(nodeId));
        writer.write("\t [");
        writer.write(node.graphString());
        writer.write("];");
        writer.newLine();
    }

    private void writeEdge(Node from, Node to, Map<Node, Integer> ids, BufferedWriter writer) throws IOException {
        Integer nodeId = ids.get(from);
        Integer childId = ids.get(to);
        writer.write(nodeId + " -- " + childId + ";");
        writer.newLine();
    }

    void write(Node root, Map<Node, Integer> ids) throws IOException {
        try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath.toFile())))){
            writer.write("graph {");
            writer.newLine();

            Stack<Node> nodes = new Stack<>();
            nodes.add(root);

            Set<Node> visited = new HashSet<>();
            while (!nodes.isEmpty()) {
                Node node = nodes.pop();
                if (!visited.contains(node)) {
                    visited.add(node);
                    writeNode(node, ids, writer);

                    List<Node> children = node.children();
                    for (Node child : children) {
                        nodes.push(child);
                        writeEdge(node, child, ids, writer);
                    }
                }
            }
            writer.write("}");
        }
    }
}
