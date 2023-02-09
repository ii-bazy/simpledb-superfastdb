# SuperFastDB (Dominik Kowalczyk, Bartosz Stefaniak)

## Requierments

    sudo apt update && sudo apt install bazel

    yay -Syu && yay -S bazel

## Running

### Available flags
    covert - Described in section 'Adding new table'
    types - Described in section 'Adding new table'
    schema_path={path} - Path to schema
    benchmark - Described in 'Benchmark'
    explain - Should print execution plan
    use_materialize - Should materialize inner loop in join
    materialize_size={number} - Available memory for materialization in MB
    use_join_optimization - Use Selinger's algorithm to optimize joins order

### Benchmark
    
In order to run benchmark use:

    bazel run --config=opt //src:main -- --benchmark

Then you can disable or enable optimizations.

Run without any optimizations:

    bazel run --config=opt //src:main -- --benchmark --use_join_optimization=0  --use_materialize=0 

Run without materialization:

    bazel run --config=opt //src:main -- --benchmark --use_materialize=0 

Run with 32MB as materialization size:

    bazel run --config=opt //src:main -- --benchmark --use_materialize  --materialize_size=32

Running main file:

    bazel run --config=opt //src:main --  --explain --schema_path=data/schema.txt 

Then you can specify name of table that you want to print.
If you want to add new table look [here](#adding-new-table).

## Testing

There are some tests in [tests](tests).
We are still working on it.

To run all tests:

    bazel test --config=opt //tests:all

To run specific test:

    bazel test --config=opt //tests:<test name>

e.g.

    bazel test --config=opt //tests:TupleTest

## Adding new table

First you need to declare new schema inside [data/schema.txt](data/schema.txt).

Then create file with .txt extension inside [data/](data/).

Then you have to convert .txt into binary file.

    bazel run //src:main -- --convert=<full_path_to_table.txt> --types="<list_of_types_in_table>"

example for existing table2.

     bazel run //src:main -- --convert=<path>/data/table2.txt --types="int,int,string,string" 

### Catalog Schema

    table_name,field_name,field_type,field_name,field_type,...

