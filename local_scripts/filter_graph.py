import re

infile = open("graph.in")
outfile = open("graph_filtered.in",'w')
lines = infile.readlines()

ignore_lines_match_regex = [
    "//dependencies",
    ".*@((?!graknlabs).)"
    "//external",
    "\[label"
    ".java"
]

# not sure why this doens't work
# regex = re.compile("({0})".format(")|(".join(ignore_lines_match_regex)))

counter = 0
for line in lines:
    m = re.search("@", line) # filter all non-local source deps
    # m1 = re.search("@((?!graknlabs).))", line) # only filter out external non-grakn deps
    m2 = re.search("//dependencies", line)
    m3 = re.search("//external", line)
    m4 = re.search("\[label", line)
    m5 = re.search("\.java", line)
    # none of the patterns may match
    if not (m or m2 or m3 or m4 or m5):
        outfile.write(line)
        counter += 1
print("Wrote {0}/{1} lines".format(counter, len(lines)))
