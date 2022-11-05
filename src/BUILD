cc_library(
    name = "SuperFastDB",
    srcs = [
        "common/Type.cpp",
        "common/Type.hpp",
        "storage/Field.hpp",
        "storage/IntField.cpp",
        "storage/IntField.hpp",
        "storage/PageId.hpp",
        "storage/RecordId.hpp",
        "storage/StringField.cpp",
        "storage/StringField.hpp",
        "storage/Tuple.hpp",
        "storage/TupleDesc.hpp",
    ],
    visibility = ["//visibility:public"],
)

cc_binary(
    name = "main",
    srcs = [
        "main.cpp",
    ],
    deps = [
        "SuperFastDB",
    ],
)
