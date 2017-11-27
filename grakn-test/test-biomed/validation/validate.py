from subprocess import check_output
import time

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print '%r  %2.2f ms' % \
                  (method.__name__, (te - ts) * 1000)
        return result
    return timed

@timeit
def runQuery(query, numResults):
    result = check_output(["graql", "console", "-n", "-k", "biomed", "-e", query]);
    assert int(result) == numResults, '\nExpected:\n%s\nActual:\n%s' % (numResults, result)

print ("Checking if data has loaded . . . ")
query = "match $x sub thing; aggregate count;";
runQuery(query, 54)

print ("Running explicit query . . .")
query = """
match  $cancer isa cancer has name \"breast cancer\";
($cancer, $sl); $sl isa stem-loop;
($sl, $m); $m isa mature;
($m, $gene); $gene isa gene;
($gene, $drug); $drug isa drug;
offset 0; limit 1;
aggregate count;
"""
runQuery(query, 1)

print ("Running query via rule . . .")
query = """
match  $cancer isa cancer has name \"breast cancer\";
$drug isa drug;
($cancer, $drug); offset 0; limit 1;
aggregate count;
"""
runQuery(query, 1)

print ("Running high degree explicit query . . .")
query = """
match $a isa cancer;
$b isa stem-loop;
$c (disease: $a, affected: $b); $c has degree >= 3;
$d isa mature;
($b, $d);
$e isa gene;
$f ($d, $e); $f has degree >= 3;
limit 1; offset 0;
aggregate count;
"""
runQuery(query, 1)

print ("Running query to check links between cancer and genes. . .")
query = """
match $a isa cancer; $b isa stem-loop;
$c (disease: $a, affected: $b);
($b, $d);
$e isa gene;
$f ($d, $e);
limit 1; offset 0;
aggregate count;
"""
runQuery(query, 1)

print ("Running query to check referenced treatments . . . ")
query = "match $x isa referenced-treatment; offset 0; limit 2; aggregate count;"
runQuery(query, 2)
