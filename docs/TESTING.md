Testing
==========

## Running Riak Test

Riak Test is a tool for running integration tests against a Riak
cluster.  See the [Riak Test README][rt_readme] for more details.

### Build a Riak/Yokozuna devrel

Make a directory to build the devrel.

    mkdir ~/testing
    cd testing

The rest is like the [Yokozuna Getting Started][yz_gs] guide but
checkout riak as `riak_yz`.  Don't start the cluster.  Just build the
devrel.

    git clone git://github.com/basho/riak.git riak_yz
    cd riak_yz

    git checkout rz-yokozuna-2

    make deps

    cd deps

    rm -rf riak_kv
    git clone git://github.com/basho/riak_kv.git
    (cd riak_kv && git checkout rz-yokozuna-3)

    cd ..
    make

    make stagedevrel

### Setup rtdev

This step will create `/tmp/rt` which is specifically setup for Riak
Test.  It provides the ability to easily rollback the cluster to a
fresh state.

    cd ~/testing
    ./riak_yz/deps/riak_test/bin/rtdev-setup-releases.sh

### Compile Yokozuna Riak Test

    cd <path-to-yokozuna>
    make

At this point Riak Test will be pulled down and compiled but for
whatever reason the Riak Test plugin will not execute on the first
pass.  A second invocation of make is required.

    make

At this point you should see `.beam` files in `riak_test/ebin`.

### Compile Yokozuna Bench Files

    cd <path-to-yokozuna>/misc/bench
    ../../rebar get-deps
    ../../rebar compile
    (cd deps/basho_bench && make)

### Add Yokozuna Config

Open `~/.riak_test.config` and add the following to the end.


    {yokozuna, [
                {rt_project, "yokozuna"},
                {rt_deps, ["<path-to-testing-dir>/riak_yz/deps"]},
                {rtdev_path, [{root, "/tmp/rt"},
                              {current, "/tmp/rt/riak_yz"}]}
               ]}.

### Run the Test

The `YZ_BENCH_DIR` is needed so Riak Test can find the files to drive
Basho Bench.

    export YZ_BENCH_DIR=<path/to/yokozuna/home>/misc/bench

Finally, run the test.

    cd <path-to-yokozuna>
    ./rebar config=yokozuna test=yokozuna_essential rt_run | tee rtrun.out

[rt_readme]: https://github.com/basho/riak_test/blob/master/README.md

[yz_gs]: https://github.com/rzezeski/yokozuna#getting-started
