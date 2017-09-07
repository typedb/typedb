from subprocess import check_output

print ("Checking if data has loaded . . . ")
result = check_output(["graql.sh", "-k", "biomed", "-e", "match $x sub thing; aggregate count;"]);
assert int(result) = 1, "Type count mismatch. Sample schema not loaded correctly"

print ("Running explicit query . . .")
query = "match  $cancer isa cancer has name \"breast cancer\";" \
    + "($cancer, $sl); $sl isa stem-loop;" \
    + "($sl, $m); $m isa mature;" \
    + "($m, $gene); $gene isa gene;" \
    + "($gene, $drug); $drug isa drug;" \
    + "offset 0; limit 1;" \
    + "aggregate count;"
result = check_output(["graql.sh", "-k", "biomed", "-e", query]);

print(result)



