import re
from graphviz import Digraph

# Input debug structure

debug_structure = ""
with open('debug_graph_source', 'r') as file:
    debug_structure = file.read()

def parse_debug_structure(debug_structure):
    vertices = {}
    edges = []

    # Regex patterns to extract blocks and connections
    block_pattern = re.compile(r'([VP]\[\d+\]):\s*(\w+)\((.*)', re.DOTALL)
    connection_pattern = re.compile(r'(\w+):\s*V\[(\d+)\]')
    isa_type_pattern = re.compile(r'type_:\s*(\w+)')
    comparator_pattern = re.compile(r'comparator:\s*(\w+)')

    for block in debug_structure.split('},'):
        block_match = block_pattern.search(block)
        if block_match:
            block_id, block_type, block_body = block_match.groups()

            # Handle different block types
            if block_type == "Constraint" and "Isa(" in block_body:
                # Special case for Isa nodes
                type_match = isa_type_pattern.search(block_body)
                type_name = type_match.group(1) if type_match else "Unknown"
                label = f"Isa {type_name} ({block_id[2:-1]})"
            elif block_type == "Constraint":
                label = f"{block_body.split('(', 1)[0]} ({block_id[2:-1]})"
            elif block_type == "Comparison":
                # Special case for Comparison nodes
                comp_match = comparator_pattern.search(block_body)
                comp_type = comp_match.group(1) if comp_match else "Unknown"
                label = f"Comparison {comp_type} ({block_id[2:-1]})"
            elif block_type == "Variable":
                label = f"Variable ({block_id[2:-1]})"
            else:
                continue
            vertices[block_id] = label

            # Find connections
            for conn_key, conn_target in connection_pattern.findall(block_body):
                edges.append((block_id, f"V[{conn_target}]", conn_key))

    return vertices, edges

def generate_graph(vertices, edges):
    dot = Digraph(format="png")

    # Graph layout settings for landscape-style, larger canvas
    dot.attr(rankdir="LR")
    dot.graph_attr.update(size="10,10", dpi="300", ratio="fill")

    # Add vertices
    for vertex_id, label in vertices.items():
        dot.node(vertex_id, label)

    # Add edges
    for source, target, label in edges:
        if source in vertices and target in vertices:
            dot.edge(source, target, label=label)

    return dot

# Parse the debug structure and generate graph
vertices, edges = parse_debug_structure(debug_structure)
graph = generate_graph(vertices, edges)

# Save and display the graph
graph.render("debug_graph", view=True)
