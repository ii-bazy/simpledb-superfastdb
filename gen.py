from random import randint, seed
import os
import numpy as np


def convert_table(name, types):
    # print(os.path.abspath("."))
    path = os.path.abspath(name)
    print(f"Converting table({name}) with types: {types}")
    os.system(
        "bazel run --config=opt //src:main -- "
        f"--convert={path} "
        f"--types=\"{types}\"")

#     // A(a, b) -> Tu a jest kluczem
#     // B(b, c) -> Tu b jest kluczem
#     // C(a, d) -> Tu a jest kluczem

#     // Joiny A.b == B.b
#     //       A.a == C.a
#     // D(b, e)  -> Tu b jest kluczem
#     //       A.b < D.b
#     // E(a, f)  -> Tu a jest kluczem
#     //       C.a > E.a
#     // F(d, g)  -> Tu d jest kluczem
#     //       C.d = F.d
#     // G (gowo, gowno)
#     //       A.a >= gowno AND gowno < 5


# same seed is needed for tests!
seed(1231)
np.random.seed(1231)

RANGE = 1000

for c in range(ord('A'), ord('F') + 1):
    name = f"data/{chr(c)}.txt"
    SIZE = 100
    with open(f'{name}', "w") as file:
        f = np.random.choice(np.arange(RANGE),
                             SIZE + np.random.randint(0, 1000), replace=False)
        print(f"{f.shape = }")
        s = np.random.choice(np.arange(RANGE),
                             SIZE + np.random.randint(0, 1000), replace=True)
        for i in zip(f, s):
            print(f'{i[0]},{i[1]}', file=file)
    convert_table(name, "int,int")

G_name = "data/G.txt"
G_types = "int,int"
G_size = 100

with open(G_name, "w") as file:
    f = np.random.choice(np.arange(RANGE), G_size +
                         np.random.randint(0, 1000), replace=True)
    print(f"{f.shape = }")
    s = np.random.choice(np.arange(RANGE), G_size +
                         np.random.randint(0, 1000), replace=True)
    print(f"{s.shape = }")
    for i in zip(f, s):
        print(f'{i[0]},{i[1]}', file=file)
convert_table(G_name, G_types)

# T1_SIZE = 10_000
# T1_name = "data/T1.txt"
# T1_types = "int,int,int,string"

# with open(T1_name, "w") as file:
#     # random string at the end is just to make that table bigger!
#     for i in range(T1_SIZE):
#         print(f"{i},{randint(-100, 100)},{randint(-100, 100)},a", file=file)


# convert_table(T1_name, T1_types)

# T2_SIZE = 50
# T2_name = "data/T2.txt"
# T2_types = "int,int"

# with open(T2_name, "w") as file:
#     for i in range(T2_SIZE):
#         print(f"{i},{randint(-100, 100)}", file=file)


# convert_table(T2_name, T2_types)
