load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

git_repository(
    name = "com_google_googletest",
    branch = "main",
    remote = "https://github.com/google/googletest",
)

http_archive(
    name = "com_github_gflags_gflags",
    sha256 = "34af2f15cf7367513b352bdcd2493ab14ce43692d2dcd9dfc499492966c64dcf",
    strip_prefix = "gflags-2.2.2",
    urls = ["https://github.com/gflags/gflags/archive/v2.2.2.tar.gz"],
)

http_archive(
    name = "com_github_google_glog",
    sha256 = "122fb6b712808ef43fbf80f75c52a21c9760683dae470154f02bddfc61135022",
    strip_prefix = "glog-0.6.0",
    urls = ["https://github.com/google/glog/archive/v0.6.0.zip"],
)

http_archive(
    name = "com_google_absl",
    sha256 = "54707f411cb62a26a776dad5fd60829098c181700edcd022ea5c2ca49e9b7ef1",
    strip_prefix = "abseil-cpp-20220623.1",
    urls = ["https://github.com/abseil/abseil-cpp/archive/refs/tags/20220623.1.zip"],
)

http_archive(
    name = "bazel_skylib",
    sha256 = "f7be3474d42aae265405a592bb7da8e171919d74c16f082a5457840f06054728",
    urls = ["https://github.com/bazelbuild/bazel-skylib/releases/download/1.2.1/bazel-skylib-1.2.1.tar.gz"],
)

http_archive(
    name = "rules_foreign_cc",
    sha256 = "076b8217296ca25d5b2167a832c8703cc51cbf8d980f00d6c71e9691876f6b08",
    strip_prefix = "rules_foreign_cc-2c6262f8f487cd3481db27e2c509d9e6d30bfe53",
    url = "https://github.com/bazelbuild/rules_foreign_cc/archive/2c6262f8f487cd3481db27e2c509d9e6d30bfe53.tar.gz",
)

load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")

rules_foreign_cc_dependencies()

_ALL_CONTENT = """\
filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
"""

http_archive(
    name = "sql_parser",
    build_file_content = _ALL_CONTENT,
    patch_args = ["-p1"],
    patches = ["patches/sql_parser/makefile.patch"],
    sha256 = "6917c6a78dbb01cd55743ddcf1436fef6ae190ef51bcc6cf2e9ba67c9bbff39a",
    strip_prefix = "sql-parser-master",
    urls = ["https://github.com/hyrise/sql-parser/archive/refs/heads/master.zip"],
)
