from random import randint, seed
import os


def convert_table(name, types):
    # print(os.path.abspath("."))
    path = os.path.abspath(name)
    print(f"Converting table({name}) with types: {types}")
    os.system(
        "bazel run --config=opt //src:main -- "
        f"--convert={path} "
        f"--types=\"{types}\" ")


# same seed is needed for tests!
seed(1231)

T1_SIZE = 100_000
T1_name = "data/T1.txt"
T1_types = "int,int,int,string"

with open(T1_name, "w") as file:
    # random string at the end is just to make that table bigger!
    for i in range(T1_SIZE):
        print(f"{i},{randint(-100, 100)},{randint(-100, 100)},a", file=file)


convert_table(T1_name, T1_types)

T2_SIZE = 3_000
T2_name = "data/T2.txt"
T2_types = "int,int"

with open(T2_name, "w") as file:
    for i in range(T2_SIZE):
        print(f"{i},{randint(-100, 100)}", file=file)


convert_table(T2_name, T2_types)
