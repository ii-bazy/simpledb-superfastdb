# SuperFastDB (Dominik Kowalczyk, Bartosz Stefaniak)

## Requierments

    sudo apt update && sudo apt install bazel

## Running

Running main file:

    bazel run //src:main 

Then you can specify name of table that you want to print.
If you want to add new table look [here](#adding-new-table).

## Testing

There are some tests in [tests](tests).
We are still working on it.

To run all tests:

    bazel test //tests:all

To run specific test:

    bazel test //tests:<test name>

e.g.

    bazel test //tests:TupleTest

## Adding new table

First you need to declare new schema inside [data/schema.txt](data/schema.txt).

Then create file with .txt extension inside [data/](data/).

Then you have to convert .txt into binary file.

    bazel run //src:main -- --conver=<full_path_to_table.txt> --types="<list_of_types_in_table>"

example for existing table2.

     bazel run //src:main -- --convert=<path>/data/table2.txt --types="int,int,string,string" 

### Catalog Schema

    table_name,field_name,field_type,field_name,field_type,...
