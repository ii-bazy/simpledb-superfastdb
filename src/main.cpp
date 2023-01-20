#include <gflags/gflags.h>
#include <glog/logging.h>

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
#include "src/storage/BufferPool.hpp"
#include "src/storage/IntField.hpp"
#include "src/storage/Tuple.hpp"
#include "src/storage/TupleDesc.hpp"
#include "src/execution/logical_plan/logical_plan.hpp"
#include "src/parser.hpp"

DEFINE_string(convert, "", "Path to file to convert to binary.");
DEFINE_string(types, "", "Types of columns in table.");

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

    // for (auto row : tuples) {
    //     for (auto i : row) {
    //         std::cerr << i << " ";
    //     }
    //     std::cerr << "\n";
    // }

    auto binary_file =
        file_name.substr(0, file_name.find_last_of(".txt") - 3) + ".dat";
    std::cerr << binary_file << "\n";
    std::ofstream out_file(binary_file, std::ios::binary | std::ios::out);

    const int bytes_per_page = BufferPool::get_page_size();
    const int tuples_per_page =
        (BufferPool::get_page_size() * 8) / (tuple_size * 8 + 1);
    // std::cerr << "Tuples per page: " << tuples_per_page << "\n";
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

    Database::get_catalog().load_schema("data/schema.txt");

    const auto query = "SELECT col1, col2 FROM table1 WHERE col1 < 10";

    
    std::unique_ptr<Parser> parser = std::make_unique<Parser>();
    auto lp = parser->ParseQuery(query).value(); // I'm ok with crash.
    
    auto it = lp.PhysicalPlan(TransactionId());

    std::cout << it->get_tuple_desc()->to_string() << "\n";
    // for (int i = 0; i < 10; ++i) {
    for (auto itt : *it) {
        std::cerr << itt->to_string() << "\n";
    }
    

    // lt.PhysicalPlan(TransactionId());
    

    return 0;
    while (true) {
        std::string table_name;
        std::cout << "Table name to seq_scan:";
        std::cout.flush();

        std::cin >> table_name;
        TransactionId tid;
        try {
            // std::unique_ptr<Field> operand1 = std::make_unique<IntField>(10);
            // std::unique_ptr<OpIterator> t1 = std::make_unique<Filter>(
            //     Predicate(0, OpType::LESS_THAN, operand1.get()),
            //     std::make_unique<SeqScan>(
            //         tid, Database::get_catalog().get_table_id(table_name),
            //         "T1"));

            // std::cerr << "T1 table:\n";
            // for (auto it : *t1) {
            //     std::cerr << it->to_string() << "\n";
            // }

            // std::unique_ptr<Field> operand2 = std::make_unique<IntField>(5);
            // std::unique_ptr<OpIterator> t2 = std::make_unique<Filter>(
            //     Predicate(0, OpType::LESS_THAN, operand2.get()),
            //     std::make_unique<SeqScan>(
            //         tid, Database::get_catalog().get_table_id(table_name),
            //         "T2"));

            // std::cerr << "T2 table:\n";
            // for (auto it : *t2) {
            //     std::cerr << it->to_string() << "\n";
            // }

            // auto seq_scan = std::make_unique<Join>(Join(JoinPredicate(0,
            // OpType::EQUALS, 0),
            //                      std::move(t1), std::move(t2)));

            // std::cerr << "Join table:\n";
            // for (auto it : *seq_scan) {
            //     std::cerr << it->to_string() << "\n";
            // }

            // auto agg = Aggregate(std::move(seq_scan), 0, 1,
            // AggregatorOp::COUNT); Filter seq_scan =(

            //     std::make_unique<SeqScan>(
            //         tid, Database::get_catalog().get_table_id(table_name),
            //         ""));

            auto seq_scan = SeqScan(
                tid, Database::get_catalog().get_table_id(table_name), "");

            std::cout << seq_scan.get_tuple_desc()->to_string() << "\n";
            for (int i = 0; i < 10; ++i) {
                for (auto it : seq_scan) {
                    // std::cerr << it->to_string() << "\n";
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Error:" << e.what() << "\n";
        }
    }

    return 0;
}

// bazel test //tests:TupleTest
// bazel build //src:main
// bazel run //src:main
//  bazel run //src:main --
//  --convert=/home/domiko/Documents/UWR/simpledb-superfastdb/data/table1.txt
//  --types="int,int" --logtostderr=1