# Run with `resolution_diagrams path/to/test/sandbox/ ./`. This will find the .dot files there, build them into svgs,
# and save them here. You need to use `--sandbox_debug` when you run bazel tests.
for x in $1*.dot
do
  b=$(basename $x)
  echo $b
cat $x | dot -Tsvg > $2$b.svg
done