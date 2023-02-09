#include <gflags/gflags.h>
#include <glog/logging.h>
#include "src/flags.hpp"

#include <iostream>
#include <numeric>
#include <sstream>

#include "src/common/Catalog.hpp"
#include "src/common/Type.hpp"
#include "src/execution/Aggregate.hpp"
#include "src/execution/Filter.hpp"
#include "src/execution/IntegerAggregator.hpp"
#include "src/execution/Join.hpp"
#include "src/execution/JoinPredicate.hpp"
#include "src/execution/Operator.hpp"
#include "src/execution/SeqScan.hpp"
#include "src/execution/StringAggregator.hpp"
#include "src/execution/logical_plan/logical_plan.hpp"
#include "src/parser.hpp"
#include "src/storage/BufferPool.hpp"
#include "src/storage/IntField.hpp"
#include "src/storage/Tuple.hpp"
#include "src/storage/TupleDesc.hpp"

std::vector<std::string> split_line(std::string line, char delimeter = ',') {
    std::stringstream ss(line);
    std::vector<std::string> res;
    while (ss.good()) {
        std::string sub;
        std::getline(ss, sub, delimeter);
        res.push_back(std::move(sub));
    }
    return res;
}

void convert(const std::string file_name, std::vector<const Type*> types) {
    const int tuple_size = std::accumulate(
        types.begin(), types.end(), 0,
        [](const int acum, const Type* t) { return acum + t->get_len(); });

    std::vector<std::vector<std::string>> tuples;

    std::string line;
    std::fstream file(file_name);
    while (std::getline(file, line)) {
        tuples.push_back(split_line(line));
    }

    auto binary_file =
        file_name.substr(0, file_name.find_last_of(".txt") - 3) + ".dat";
    std::cerr << binary_file << "\n";
    std::ofstream out_file(binary_file, std::ios::binary | std::ios::out);

    const int bytes_per_page = BufferPool::get_page_size();
    const int tuples_per_page =
        (BufferPool::get_page_size() * 8) / (tuple_size * 8 + 1);

    const int header_bytes = (tuples_per_page + 7) / 8;
    const int pages = (static_cast<int>(tuples.size()) + tuples_per_page - 1) /
                      tuples_per_page;

    LOG(INFO) << "Tuple_cnt " << tuples.size();
    LOG(INFO) << "Tuple_per_page " << tuples_per_page;

    // std::cerr << "Header bytes: " << header_bytes << "\n";

    for (int page = 0; page < pages; ++page) {
        LOG(INFO) << "Writing page: " << page << "\n";
        const int start_index = page * tuples_per_page;
        const int end_index = std::min(start_index + tuples_per_page,
                                       static_cast<int>(tuples.size()));

        LOG(INFO) << "Start " << start_index << " end " << end_index;
        // LOG(INFO) << "header bytes"

        std::vector<char> header(header_bytes, 0);

        int bytes_to_write = bytes_per_page;

        for (int i = start_index; i < end_index; ++i) {
            const int index = i - start_index;
            LOG(INFO) << "index: " << index;
            header[index / 8] |= (1 << index % 8);
        }

        for (const auto p : header) {
            out_file.write(&p, sizeof(p));
            bytes_to_write -= sizeof(p);
        }

        for (int i = start_index; i < end_index; ++i) {
            // if (typ)
            for (int j = 0; j < static_cast<int>(types.size()); ++j) {
                if (types[j] == Type::INT_TYPE()) {
                    // std::cerr << "Parse int\t";
                    int temp = std::stoi(tuples[i][j]);
                    out_file.write(reinterpret_cast<char*>(&temp), sizeof(int));
                    bytes_to_write -= sizeof(int);
                } else if (types[j] == Type::STRING_TYPE()) {
                    // std::cerr << "Parse str\t";
                    int length =
                        std::min<int>(static_cast<int>(tuples[i][j].size()),
                                      Type::STRING_LEN);
                    out_file.write(reinterpret_cast<char*>(&length),
                                   sizeof(int));
                    bytes_to_write -= sizeof(int);
                    tuples[i][j].resize(Type::STRING_LEN, '0');
                    out_file.write(tuples[i][j].data(), Type::STRING_LEN);
                    bytes_to_write -= Type::STRING_LEN;
                } else {
                    throw std::invalid_argument("INVALID type");
                }
            }
            // std::cerr << "\n";
        }

        LOG(INFO) << "Padding: " << bytes_to_write;

        const std::string padding(bytes_to_write, 0);
        out_file.write(reinterpret_cast<const char*>(padding.data()),
                       bytes_to_write);
    }

    out_file.close();
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    if (FLAGS_convert.size() > 0u) {
        std::vector<const Type*> types;
        const auto tokens = split_line(FLAGS_types);
        for (const auto& type : tokens) {
            if (type == "int") {
                types.push_back(Type::INT_TYPE());
            } else if (type == "string") {
                types.push_back(Type::STRING_TYPE());
            } else {
                throw std::invalid_argument("Unknown type: " + type);
            }
        }
        convert(FLAGS_convert, types);
        return 0;
    }

    if (FLAGS_benchmark) {
        std::vector<std::string> queries{
            "SELECT count(age) FROM table3 WHERE country = 'Italy'",
            "SELECT age, count(age) FROM table3 WHERE country = 'Italy' GROUP BY age",
            "SELECT * FROM table3, table3 t3 WHERE t3.age = 4 AND table3.age >= t3.age",
            "SELECT * FROM table3, table1, table3 t3 WHERE t3.age <= table3.age AND table3.age < table1.col1 AND t3.age = 4",
            "SELECT * FROM table3 WHERE table3.country = 'Italy'",
            "SELECT * FROM T2 WHERE T2.col1 <= 40 AND T2.col2 > 0",
            "SELECT * FROM T1, T2 WHERE T1.col1 <= 10 AND T2.col2 >= T1.col2",
            "SELECT * FROM T1, T2 t2, T2 WHERE T1.col1 <= 100 AND t2.col2 = T1.col2 AND T2.col2 >= T1.col2",
            "SELECT * FROM A, B, C, D, E, F, G WHERE A.b = B.b AND A.a = C.a AND A.b < D.b AND C.a > E.a AND C.d = F.d AND A.a >= G.h AND G.j < 57",
            "SELECT * FROM A, B, C, D, E, F, G WHERE C.d = F.d AND A.b = B.b AND A.a = C.a AND A.b < D.b AND C.a > E.a AND A.a >= G.h AND G.j < 57",
        };

        Database::get_catalog().load_schema("data/schema.txt");

        std::unique_ptr<Parser> parser = std::make_unique<Parser>();

        uint64_t total_time_ms = 0;
        for (const auto& query : queries) {
            try {
                auto lp = parser->ParseQuery(query).value();  // I'm ok with crash.
                auto it = lp.PhysicalPlan(TransactionId());

                if (FLAGS_explain) {
                    it->explain(std::cout, 0);
                }

                auto start_time = std::chrono::steady_clock::now();
                int total_tuples = 0;
                for (auto itt : *it) {
                    total_tuples += 1;
                }
                std::cout << "Number of tuples: " << total_tuples << "\t";

                auto end_time = std::chrono::steady_clock::now();
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                end_time - start_time)
                                .count();
                std::cout << "Query took " << ms << "ms\n";
                total_time_ms += ms;

            } catch (const std::exception& e) {
                std::cerr << "Benchmark error with query: " << query 
                        << " : " << e.what() << "\n";
            }
        }

        std::cout << "All queries took " << total_time_ms << "ms\n";

        return 0;
    }

    if (FLAGS_schema_path.size() == 0) {
        std::cerr << "schema_path is required!\n";
        return 1;
    }

    Database::get_catalog().load_schema(FLAGS_schema_path);

    std::unique_ptr<Parser> parser = std::make_unique<Parser>();
    while (true) {
        std::cout << "> ";
        std::string query;
        if (!std::getline(std::cin, query)) {
            break;
        }
        try {
            auto lp = parser->ParseQuery(query).value();  // I'm ok with crash.
            auto it = lp.PhysicalPlan(TransactionId());

            if (FLAGS_explain) {
                it->explain(std::cerr, 0);
            }

            auto start_time = std::chrono::steady_clock::now();
            std::cerr << it->get_tuple_desc()->to_string() << "\n";
            for (auto itt : *it) {
                std::cout << itt->to_string() << "\n";
            }

            auto end_time = std::chrono::steady_clock::now();
            std::cerr << "Query took "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(
                             end_time - start_time)
                             .count()
                      << "ms\n";

        } catch (const std::exception& e) {
            std::cerr << "Error:" << e.what() << "\n";
        }
    }

    return 0;
}