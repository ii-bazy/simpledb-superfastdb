cc_library(
    name = "SuperFastDB",
    srcs = [
        "common/Catalog.cpp",
        "common/Catalog.hpp",
        "common/Database.cpp",
        "common/Database.hpp",
        "common/Permissions.hpp",
        "common/Type.cpp",
        "common/Type.hpp",
        "execution/OpIterator.hpp",
        "execution/SeqScan.hpp",
        "storage/BufferPool.cpp",
        "storage/BufferPool.hpp",
        "storage/DbFile.hpp",
        "storage/Field.hpp",
        "storage/HeapFile.cpp",
        "storage/HeapFile.hpp",
        "storage/HeapPage.cpp",
        "storage/HeapPage.hpp",
        "storage/HeapPageId.hpp",
        "storage/IntField.cpp",
        "storage/IntField.hpp",
        "storage/Page.hpp",
        "storage/PageId.hpp",
        "storage/RecordId.hpp",
        "storage/StringField.cpp",
        "storage/StringField.hpp",
        "storage/Tuple.hpp",
        "storage/TupleDesc.hpp",
        "transaction/TransactionId.hpp",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_google_glog//:glog",
    ],
)

cc_binary(
    name = "main",
    srcs = [
        "main.cpp",
    ],
    data = [
        "//data",
    ],
    deps = [
        "SuperFastDB",
        "@com_github_gflags_gflags//:gflags",
        "@com_github_google_glog//:glog",
    ],
)
