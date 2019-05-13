// This is a self-contained C++ source file skecthing some of the Spanner API
// that we might want to use. This file can be compiled and run with:
//
//   $ g++ -std=c++17 jgm.cc -o jgm && ./jgm
//
// The primary public API points shown here are:
//
// * spanner::Cell - A strongly typed value in a Spanner row-column.
// * spanner::Row - A range of spanner::Cells, with some additional methods for
//                  conveniently accessing the values contained in the cells
//                  via their position and/or column name.
//
// * spanner::Key - A range of cell values for the columns in the "index".
// * spanner::KeySet - A collection of Keys and KeyRanges for an index.
//
// * spanner::Client - Represents a connection to a Spanner database.
//                     This class only has a Read() method currently. This one
//                     Read() method implements both streaming and
//                     non-streaming reads.
// * spanner::ResultStream - A "stream" of spanner::Rows.
//
// Look at the main() function at the bottom to see what the usage looks like.
//
// NOTES:
// * This file relies on C++17 only because of std::variant (and its friends).
//   We could get this from Abseil, or implement it ourselves.
// * This is currently lacking a Transaction class; I plan to add it next.
//
// QUESTIONS:
// * Does Read() require some columns to be specified, or is there a way to say
//   all columns?
// * We'd like to expose a single client.Read() function that has a simple API
//   for users but always calls the StreamingRead() RPC. Is this OK, or is
//   there a performance reason that we might need to use the non-streaming
//   Read() RPC? Same Q for ExecuteSql().
//
#include <cassert>
#include <chrono>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <variant>
#include <vector>

// Simple, fake StatusOr.
template <typename T>
using StatusOr = std::optional<T>;
struct Status {};

namespace spanner {
namespace internal {

// Forward declared so it can be used by ARRAY and STRUCT.
struct Value;

// Spanner's defined types
using BOOL = bool;
using INT64 = std::int64_t;
using FLOAT64 = double;
using STRING = std::string;
using ARRAY = std::vector<std::shared_ptr<Value>>;
using STRUCT = std::map<std::string, std::shared_ptr<Value>>;
/* using TIMESTAMP = XXX; */
/* using DATE = XXX; */
/* using BYTES = XXX; */

// Converts a std::variant<Ts...> to a std::variant<std::optional<Ts>...>
template <typename... Ts>
struct Optionalize;
template <typename... Ts>
struct Optionalize<std::variant<Ts...>> {
  using type = std::variant<std::optional<Ts>...>;
};

// A type-safe container for any of the types that Spanner can store.
using ValueVariant = std::variant<BOOL, INT64, FLOAT64, STRING, ARRAY, STRUCT>;
struct Value : ValueVariant {
  using ValueVariant::ValueVariant;
};

// Just like ValueVariant, but each type is wrapped in a std::optional so that
// we can store the notion of a null cell while preserving the cell's type.
using CellValue = Optionalize<ValueVariant>::type;

}  // namespace internal

//
// USER-FACING PUBLIC API
//

// Represents a strongly-typed value at a Spanner row-column intersection.
// Cells have a column name and a value. Provides type-safe accessors for the
// contained value, which may be "null".
class Cell {
 public:
  template <typename T>
  explicit Cell(std::string c, T&& v)
      : column_(std::move(c)),
        value_{std::optional<std::decay_t<T>>(std::forward<T>(v))} {}

  std::string column() const { return column_; }

  // Type-safe getter, allows
  //   auto opt = cell.get<int64_t>();
  template <typename T>
  std::optional<T> get() const {  // XXX: This throws if T is the wrong type.
    return std::get<std::optional<T>>(value_);
  }

  // Returns true if the cell's type is T.
  //   bool b = cell.is<int64_t>();
  template <typename T>
  bool is() const {
    return std::holds_alternative<std::optional<T>>(value_);
  }

 private:
  std::string column_;
  internal::CellValue value_;
};

// Represents a range of Cells. Provides type-safe convenience member functions
// for accessing Cells by their column name or index offset within the row.
class Row {
 public:
  using iterator = std::vector<Cell>::iterator;
  using const_iterator = std::vector<Cell>::const_iterator;

  iterator begin() { return v_.begin(); }
  iterator end() { return v_.end(); }
  const_iterator begin() const { return v_.begin(); }
  const_iterator end() const { return v_.end(); }

  void AddCell(Cell cell) { v_.push_back(std::move(cell)); }

  template <typename T>
  std::optional<T> get(std::string const& col) const {
    for (auto& cell : v_) {  // XXX: maybe a better non-linear search?
      if (col == cell.column()) return cell.get<T>();
    }
    return std::nullopt;
  }

  template <typename T>
  std::optional<T> get(size_t index) const {
    return index < v_.size() ? v_[index].get<T>() : std::nullopt;
  }

  template <typename T>
  bool is(std::string const& col) const {
    for (auto& cell : v_) {
      if (col == cell.column()) return cell.is<T>();
    }
    return false;  // XXX
  }
  template <typename T>
  bool is(size_t index) {
    return index < v_.size() ? v_[index].is<T>() : false;  // XXX
  }

 private:
  std::vector<Cell> v_;
};

// Represents a range of cell values that correspond to a DB table index.
// Similar to a Row (both contain a range of cell values), but this doesn't
// have column names, nor any of the accessors. Maybe we want to have some
// other accessors here?
//
// TODO: Need some way to specify a null cell
class Key {
 public:
  template <typename... Ts>  // XXX: Add appropriate enablers.
  explicit Key(Ts&&... ts)
      : v{std::optional<std::decay_t<Ts>>(std::forward<Ts>(ts))...} {}

 private:
  std::vector<internal::CellValue> v;
};

// Represents the name of a database table index, along with a bunch of Keys
// and (TODO) key ranges for the specified index.
//
// TODO: Add support for key ranges as well.
// TODO: Add support for "all"
// TODO: Do we want some fancy syntax to build key sets and ranges nicely?
//       Maybe op+, op+=, etc?
class KeySet {
 public:
  KeySet() = default;  // uses the primary index.
  explicit KeySet(std::string index_name) : index_(std::move(index_name)) {}

  void Add(Key key) { keys.push_back(std::move(key)); }

 private:
  std::string index_;
  std::vector<Key> keys;
};

class ResultStats {
  // XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXxxxxxx
};

// Represents a stream of Row objects. Actually, a stream of StatusOr<Row>.
// This will be returned from the Client::Read() function.
// TODO: Implement a real stream, not just a vector, like we have here.
class ResultStream {
 public:
  using value_type = StatusOr<Row>;
  using iterator = std::vector<value_type>::iterator;
  using const_iterator = std::vector<value_type>::const_iterator;

  ResultStream() = default;
  explicit ResultStream(std::vector<Row> v) {
    for (auto&& e : v) v_.emplace_back(e);
  }

  iterator begin() { return v_.begin(); }
  iterator end() { return v_.end(); }
  const_iterator begin() const { return v_.begin(); }
  const_iterator end() const { return v_.end(); }

  // XXX: Can only be called after consuming the whole stream.
  std::optional<ResultStats> stats() const {
    return {};
  }

 private:
  std::vector<value_type> v_;
};

// Represents an SQL statement with optional parameters. Parameter placeholders
// may be specified in the sql string using `@` followed by the parameter name.
// ... follow Spanner's docs about this.
class SqlStatement {
 public:
  // XXX: Can/should we re-use Cell here? It could be made more generic so
  // that it's column attribute is just a generic "name", not necessarily a
  // column?
  using param_type = std::vector<Cell>;
  explicit SqlStatement(std::string sql, param_type params)
      : sql_(std::move(sql)), params_(std::move(params)) {}

  std::string sql() const { return sql_; }
  param_type params() const { return params_; }

 private:
  std::string sql_;
  param_type params_;
};

class Client;  // Used below in Transaction's friend declarations.

// Represents a Spanner transaction. All transaction types (read-only,
// read-write, etc.) are represented by the same Transaction type. Callers
// create different types of Transaction via factory functions in the Client
// class, e.g., `Transaction tx = client.StartReadOnlyTransaction()`.
class Transaction {
 public:
  // Value type.
  // No default c'tor, but defaulted copy, assign, move, etc.
  // XXX: Is there any public API for users?

  friend bool operator==(Transaction const& a, Transaction const& b) {
    return a.session_ == b.session_ && a.id_ == b.id_;
  }
  friend bool operator!=(Transaction const& a, Transaction const& b) {
    return !(a == b);
  }

 private:
  friend class Client;
  friend std::string SerializeTransaction(Transaction);
  friend StatusOr<Transaction> DeserializeTransaction(std::string);
  friend StatusOr<Client> MakeClient(Transaction);

  // Private default c'tor; represents single-use transaction
  Transaction() = default;
  explicit Transaction(std::string session, std::string id)
      : session_(std::move(session)), id_(std::move(id)) {}

  std::string session_;
  std::string id_;
};

std::string SerializeTransaction(Transaction tx) {
  // TODO: Use proto or something better to serialize
  return tx.session_ + "/" + tx.id_;
}

StatusOr<Transaction> DeserializeTransaction(std::string s) {
  // TODO: Properly deserialize.
  auto pos = s.find('/');
  if (pos == std::string::npos) return std::nullopt;
  return Transaction(s.substr(0, pos), s.substr(pos + 1));
}

class Mutation {
  // XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXxxxxx
};

// The options passed to PartitionRead() and PartitionQuery().
struct PartitionOptions {
  int64_t partition_size_bytes;  // currently ignored
  int64_t max_partitions;        // currently ignored
};

class ReadPartition {
 public:
  // ...
 private:
   std::string token_;
   Transaction tx_;
   std::string table_;
   KeySet keys_;
   std::vector<std::string> columns_;
};

class QueryPartition {
 public:
  // ...
 private:
   std::string token_;
   Transaction tx_;
   SqlStatement statement_;
};

// Represents a connection to a Spanner database.
class Client {
 public:
  // move-only
  Client(Client&&) = default;
  Client& operator=(Client&&) = default;
  Client(Client const&) = delete;
  Client& operator=(Client const&) = delete;

  //
  // Transactions
  //

  struct ReadOnlyOptions {
    // ...
  };
  StatusOr<Transaction> StartReadOnlyTransaction(ReadOnlyOptions = {}) {
    // TODO: Call Spanner's BeginTransaction() rpc.
    static int64_t id = 0;
    return Transaction("dummy-session", "read-only-" + std::to_string(id++));
  }

  StatusOr<Transaction> StartReadWriteTransaction() {
    // TODO: Call Spanner's BeginTransaction() rpc.
    static int64_t id = 0;
    return Transaction("dummy-session", "read-write-" + std::to_string(id++));
  }

  // Note: These transactions can only be used with ExecuteSql() (I think).
  // I *think* this has nothing to do with other "partitioned" methods.
  StatusOr<Transaction> StartPartitionedDmlTransaction() {
    // TODO: Call Spanner's BeginTransaction() rpc.
    static int64_t id = 0;
    return Transaction("dummy-session", "partitioned-dml-" + std::to_string(id++));
  }

  // QQQ: How do we support those "begin" transactions that are implicitly
  // created on the first request and thus avoid an RPC to
  // BeginTransaction()???

  // XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXxxxxx
  StatusOr<std::chrono::system_clock::time_point> Commit(
      Transaction tx, std::vector<Mutation> mutations) {
    // TODO: Call Spanner's Commit() rpc.
    return std::chrono::system_clock::now();
  }

  Status Rollback(Transaction tx) {
    // TODO: Call Spanner's Rollback() rpc.
    return {};
  }

  //
  // Read()
  //

  // Reads the columns for the given keys from the specified table. Returns a
  // stream of Rows.
  // TODO: Add support for "limit" integer.
  // TODO: Add support for "partition" token.
  ResultStream Read(Transaction tx, std::string table, KeySet keys,
                    std::vector<std::string> columns) {
    // Fills in two rows of dummy data.
    std::vector<Row> v;
    int64_t data = 1;
    v.push_back(Row{});
    for (auto const& c : columns) {
      v.back().AddCell(Cell(c, data++));
    }
    v.push_back(Row{});
    for (auto const& c : columns) {
      v.back().AddCell(Cell(c, data++));
    }
    return ResultStream(v);
  }

  // Same as Read() above, but implicitly uses a single-use Transaction.
  ResultStream Read(std::string table, KeySet keys,
                    std::vector<std::string> columns) {
    auto single_use = Transaction();  // XXX: Some indication of single-use tx
    return Read(std::move(single_use), std::move(table), std::move(keys),
                std::move(columns));
  }

  // NOTE: Requires a read-only or lazy transaction
  StatusOr<std::vector<ReadPartition>> PartitionRead(
      Transaction tx, std::string table, KeySet keys,
      std::vector<std::string> columns, PartitionOptions opts = {}) {
    // TODO: Call Spanner's PartitionRead() RPC.
    return {};
  }

  ResultStream Read(ReadPartition partition) {
    // TODO: Call Spanner's StreamingRead RPC with the data in `partition`.
    return {};
  }

  //
  // ExecuteSql
  //

  // TODO: Add support for QueryMode
  // TODO: Add support for "partition" token.
  ResultStream ExecuteSql(Transaction tx, SqlStatement statement) {
    auto columns = {"col1", "col2", "col3"};
    // Fills in two rows of dummy data.
    std::vector<Row> v;
    double data = 1;
    v.push_back(Row{});
    for (auto const& c : columns) {
      v.back().AddCell(Cell(c, data));
      data += 1;
    }
    v.push_back(Row{});
    for (auto const& c : columns) {
      v.back().AddCell(Cell(c, data));
      data += 1;
    }
    return ResultStream(v);
  }

  ResultStream ExecuteSql(SqlStatement statement) {
    auto single_use = Transaction();  // XXX: Some indication of single-use tx
    return ExecuteSql(std::move(single_use), std::move(statement));
  }

  // NOTE: Requires a read-only or lazy transaction
  StatusOr<std::vector<QueryPartition>> PartitionQuery(
      Transaction tx, SqlStatement statement, PartitionOptions opts = {}) {
    // TODO: Call Spanner's PartitionRead() RPC.
    return {};
  }

  ResultStream Query(QueryPartition partition) {
    // TODO: Call Spanner's StreamingExecuteSql RPC with the data in `partition`.
    return {};
  }

  //
  // ExecuteBatchDml
  //

  // Note: Does not support single-use transactions, so no overload for that.
  // Note: statements.size() == result.size()
  std::vector<StatusOr<ResultStats>> ExecuteBatchDml(
      Transaction tx, std::vector<SqlStatement> statements) {
    // TODO: Call spanner's ExecuteBatchDml RPC.
    return {};
  }

 private:
  friend StatusOr<Client> MakeClient(std::map<std::string, std::string>);
  friend StatusOr<Client> MakeClient(Transaction tx);

  Client(std::map<std::string, std::string> labels)
      : labels_{std::move(labels)} {}
  Client(std::string session, std::map<std::string, std::string> labels)
      : sessions_{std::move(session)}, labels_{std::move(labels)} {}

  std::vector<std::string> sessions_;
  std::map<std::string, std::string> labels_;
  // grpc stubs.
};

StatusOr<Client> MakeClient(std::map<std::string, std::string> labels = {}) {
  // TODO: Make a connection to Spanner, set up stubs, etc.
  return Client(std::move(labels));
}

StatusOr<Client> MakeClient(Transaction tx) {
  // TODO: Call rpc.GetSession() using tx.session_ to get labels
  std::map<std::string, std::string> labels = {};
  return Client(std::move(tx.session_), std::move(labels));
}

}  // namespace spanner

int main() {
  StatusOr<spanner::Client> sc =
      spanner::MakeClient({{"label_key", "label_val"}});
  if (!sc) return 1;

  spanner::KeySet keys("index2");
  keys.Add(spanner::Key(1.0, true, "hello"));

  std::string const table = "MyTable";
  std::vector<std::string> const columns = {"A", "B", "C", "D", "E"};

  StatusOr<spanner::Transaction> tx = sc->StartReadOnlyTransaction();
  if (!tx) return 2;

  // Demonstrate serializing and deserializing a Transaction
  std::string data = spanner::SerializeTransaction(*tx);
  StatusOr<spanner::Transaction> tx2 = spanner::DeserializeTransaction(data);
  if (!tx2) return 3;
  assert(*tx == *tx2);
  std::cout << "Using serialized transaction: " << data << "\n";

  StatusOr<spanner::Client> sc2 = spanner::MakeClient(*tx2);

  // Uses Client::Read().
  std::cout << "\n# Using Client::Read()...\n";
  for (StatusOr<spanner::Row>& row :
       sc->Read(*tx, table, std::move(keys), columns)) {
    if (!row) {
      std::cout << "Read failed\n";
      continue;  // Or break? Can the next read succeed?
    }

    // You can access cell values via accessors on the Row. You can specify
    // either the column name or the column's index.
    assert(row->is<int64_t>("D"));
    std::optional<int64_t> d = row->get<int64_t>("D");
    std::cout << "D=" << d.value_or(-1) << "\n";

    assert(row->is<int64_t>(3));
    d = row->get<int64_t>(3);
    std::cout << "D(index 3)=" << d.value_or(-1) << "\n";

    // Additionally, you can iterate all the Cells in a Row.
    std::cout << "Row:\n";
    for (spanner::Cell& cell : *row) {
      std::cout << cell.column() << ": ";
      if (cell.is<bool>()) {
        std::cout << "BOOL(" << *cell.get<bool>() << ")\n";
      } else if (cell.is<int64_t>()) {
        std::cout << "INT64(" << cell.get<int64_t>().value_or(-1) << ")\n";
      } else if (cell.is<double>()) {
        std::cout << "FLOAT64(" << *cell.get<double>() << ")\n";
      }
      // ...
    }
  }

  // Uses Client::ExecuteSql().
  std::cout << "\n# Using Client::ExecuteSql()...\n";
  spanner::SqlStatement sql(
      "select * from Mytable where id > @msg_id and name like @name",
      {spanner::Cell("msg_id", int64_t{123}),
       spanner::Cell("name", std::string("sally"))});
  for (StatusOr<spanner::Row>& row : sc->ExecuteSql(*tx, sql)) {
    if (!row) {
      std::cout << "Read failed\n";
      continue;  // Or break? Can the next read succeed?
    }
    // Additionally, you can iterate all the Cells in a Row.
    std::cout << "Row:\n";
    for (spanner::Cell& cell : *row) {
      std::cout << cell.column() << ": ";
      if (cell.is<bool>()) {
        std::cout << "BOOL(" << *cell.get<bool>() << ")\n";
      } else if (cell.is<int64_t>()) {
        std::cout << "INT64(" << cell.get<int64_t>().value_or(-1) << ")\n";
      } else if (cell.is<double>()) {
        std::cout << "FLOAT64(" << *cell.get<double>() << ")\n";
      }
      // ...
    }
  }

  Status s = sc->Rollback(*tx);
  // assert(s)
}

