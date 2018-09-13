import argparse
import os
import shutil
import subprocess
import tarfile
import sys

cwd = os.getcwd()
cwd_list = cwd.split('/')
grakn_root = "/" + os.path.join(*cwd_list[:-2])
print(grakn_root)

parser = argparse.ArgumentParser(description="Prepare and run benchmarking of Grakn")

parser.add_argument('--config', type=str, help="config YAML file specifying schema, queries, etc.")
parser.add_argument('--build-grakn', dest='build_grakn', default=False, action="store_true", help="Repackage Grakn with `mvn package -DskipTests`, sets --unpack-tar")
parser.add_argument('--grakn-home-parent', dest="grakn_home_parent", type=str, required=False,
                    help="directory to find xxx-SNAPSHOT.tar.gz in, default: ../grakn-dist/target",
                    default=None)
parser.add_argument('--unpack-tar', dest="unpack_tar", default=False, action="store_true", help="Delete and re-untar the xxx-SNAPSHOT.tar.gz even if it exists already")

build_benchmark_args = parser.add_mutually_exclusive_group()
build_benchmark_args.add_argument('--build-benchmark', dest='build_benchmark', default=False, action="store_true", help="Build the benchmarking package")
build_benchmark_args.add_argument('--build-benchmark-alldeps', dest='build_benchmark_all', default=False, action="store_true", help="Build benchmarking and all its dependencies")

# override `concepts` tag in yaml file, don't generate any data
parser.add_argument('--no-data-generation', dest='no_data_generation', default=False, action="store_true", help="Disable data generation, use existing Grakn data. Requires --keyspace")
# override `schema` tag in yaml file, don't load a new schema
parser.add_argument('--no-load-schema', dest='no_load_schema', default=False, action="store_true", help="Use existing Grakn schema. Requires --keyspace")
# set the keyspace to use with existing data/schema
parser.add_argument('--keyspace', default=None, help="Specify a keyspace to use")

# set the name for the this particular execution
parser.add_argument('--execution-name', dest='execution_name', default="", help="A name for this specific execution of the benchmarking configuration")

# need the ignite bin folder
parser.add_argument('--ignite-dir', dest='ignite_dir', required=True, help="The full path of the `apache-ignite-fabric-x.x.x-bin` directory to find .jar files required")


args = parser.parse_args()

# --- argument sanity checks ---

if (args.no_load_schema or args.no_data_generation) and args.keyspace is None:
    print("Require --keyspace if disabling data generation or not loading a schema")
    sys.exit()

# --- end argument sanity checks ---


unpack_tar = args.unpack_tar
build_benchmark = args.build_benchmark
build_benchmark_and_deps = args.build_benchmark_all


if args.build_grakn:
    unpack_tar = True       # override and force delete/unpack
    build_benchmark = False # don't need to re-build benchmark again
    build_benchmark_and_deps = False
    print("...Building Grakn")
    grakn_build_result = subprocess.run(['mvn', 'package', '-DskipTests'], cwd=grakn_root)
    grakn_build_result.check_returncode()

if build_benchmark_and_deps:
    print("... Building Benchmarking and its dependencies")
    benchmark_and_deps_result = subprocess.run(['mvn', 'package', '-DskipTests', '--projects', 'grakn-benchmark/benchmark-runner', '-am'], cwd=grakn_root)
    benchmark_and_deps_result.check_returncode()

if build_benchmark:
    print("...Building Benchmarking (only)")
    benchmark_result = subprocess.run(['mvn', 'package', '-DskipTests', '--projects', 'grakn-benchmark/benchmark-runner'], cwd=grakn_root)
    benchmark_result.check_returncode()

# --- folder structures sanity checks ---
target_folder = os.path.join(grakn_root, *["grakn-benchmark", "benchmark-runner", "target"])
if not os.path.isdir(target_folder):
    raise Exception("Missing target folder {0}, please build benchmarking first".format(target_folder))

# --- end folder structure sanity checks ---

benchmark_jars = list(filter(lambda f: f.endswith("SNAPSHOT.jar"), os.listdir(target_folder)))
assert len(benchmark_jars) == 1, "More than 1 benchmark jar found (*SNAPSHOT.jar)"
benchmark_jar = benchmark_jars[0]
print("Benchmarking jar: {0}".format(benchmark_jar))
classpath = [os.path.join(target_folder, benchmark_jar)] 


# set default grakn_home_parent or use provided one
if args.grakn_home_parent is None:
    grakn_home_parent = os.path.join(grakn_root, *["grakn-dist", "target"])
else:
    grakn_home_parent = args.grakn_home_parent

print("Grakn-dist home parent: {0}".format(grakn_home_parent))

# check if we need to re-untar the existing distribution
existing_files = os.listdir(grakn_home_parent)
tar_gz_files = list(filter(lambda x: x.endswith('.tar.gz'), existing_files))
if len(tar_gz_files) != 1:
    print("Found {0} .tar.gz archives in grakn-home-parent directory: {1}. Require there to be only 1".format(len(tar_gz_files), grakn_home_parent))
assert len(tar_gz_files) == 1, "Found more than 1 .tar.gz file, not sure what snapshot folder to use"
tar_file = list(tar_gz_files)[0]
snapshot_dir_name = ".".join(tar_file.split('.')[:-2]) # cut off the .tar.gz
grakn_home = os.path.join(grakn_home_parent, snapshot_dir_name)


# check if we need to delete existing SNAPSHOT directory
if unpack_tar and snapshot_dir_name in existing_files:
    print("...Deleting existing snapshot directory")
    print("......Stopping existing grakn server")
    subprocess.run(['./grakn', 'server', 'stop'], cwd=grakn_home)
    delete_target = os.path.join(os.path.join(grakn_home_parent, snapshot_dir_name))
    print("......Deleting {0}".format(delete_target))
    shutil.rmtree(delete_target)
# untar if necessary
if unpack_tar or snapshot_dir_name not in existing_files:
    print("...Untarring {0}".format(tar_file))
    tar = tarfile.open(os.path.join(grakn_home_parent, tar_file))
    tar.extractall(path=grakn_home_parent) # extract to original location
    print("......Starting grakn server")
    subprocess.run(['./grakn', 'server', 'start'], cwd=grakn_home)


def recursive_search(start, accept):
    found = []
    for path, dirs, files in os.walk(start):
        for f in files:
            if accept(f):
                found.append(os.path.join(path, f))
    return found

# build the classpath with all the required jars
classpath += recursive_search(os.path.join(grakn_home, 'services', 'lib'), accept=lambda f: f.endswith('.jar'))
classpath += recursive_search(args.ignite_dir, accept=lambda f: f.endswith('.jar'))
classpath.append(os.path.join(grakn_root, 'grakn-benchmark', 'conf'))


# run benchmarking 
classpath = ":".join(classpath)
command = ['java', '-cp', classpath, 'manage.BenchmarkManager']
run_args = ['--config', args.config, '--execution-name', args.execution_name]
if args.keyspace is not None:
    run_args += ["--keyspace", args.keyspace]
if args.no_data_generation:
    run_args.append("--no-data-generation")
if args.no_load_schema:
    run_args.append("--no-load-schema")
command += run_args
print("...Running benchmarking")
print(command)
print(grakn_root)
result = subprocess.run(command, cwd=grakn_root)
result.check_returncode()
