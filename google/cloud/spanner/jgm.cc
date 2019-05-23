// This is a self-contained C++ source file skecthing some of the Spanner API
// that we might want to use. This file can be compiled and run with:
//
//   $ g++ -std=c++17 jgm.cc -o jgm && ./jgm
//
// The primary public API points shown here are:
//
// * spanner::Value - A strongly typed value in a Spanner row-column.
// * spanner::Row - A range of spanner::Values, with some additional methods for
//                  conveniently accessing the values via their position and/or
//                  column name.
//
// * spanner::Key - A range of values for the columns in the "index".
// * spanner::KeySet - A collection of Keys and KeyRanges for an index.
//
// * spanner::Client - Represents a connection to a Spanner database.
//                     This class only has a Read() method currently. This one
//                     Read() method implements both streaming and
//                     non-streaming reads.
// * spanner::ResultSet - A range of spanner::Rows.
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
// * We plan to have all of our transactions be the "implicit" type. Is that
//   fine?
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

/* // Converts a std::variant<Ts...> to a std::variant<std::optional<Ts>...> */
/* template <typename... Ts> */
/* struct Optionalize; */
/* template <typename... Ts> */
/* struct Optionalize<std::variant<Ts...>> { */
/*   using type = std::variant<std::optional<Ts>...>; */
/* }; */

/* // A type-safe container for any of the types that Spanner can store. */
/* using ValueVariant = std::variant<BOOL, INT64, FLOAT64, STRING, ARRAY, STRUCT>; */
/* struct Value : ValueVariant { */
/*   using ValueVariant::ValueVariant; */
/* }; */

}  // namespace internal

//
// USER-FACING PUBLIC API
//

class Value {
 public:
  template <typename T>   // TODO: Add appropriate enablers
  explicit Value(T&& v) {
    // ...
  }

  // Returns true if the value's type is T.
  //   bool b = value.is<int64_t>();
  template <typename T>
  bool is() const {
    return {};
  }

  // Type-safe getter, allows
  //   auto opt = value.get<int64_t>();
  //
  // Returns nullopt if the "null" value.
  // Crashes (or UB) if T is the wrong type.
  template <typename T>
  std::optional<T> get() const {
    return {};
  }

 private:
   // Basically holds a spanner Value proto.
};

class ColumnBase {
  public:
   std::string name() const { return {}; }

   template <typename T>
   bool is() const {
     return {};
   }

   friend bool operator==(ColumnBase a, ColumnBase b) {
     return a.name_ == b.name_ && a.index_ == b.index_;
   }

   friend bool operator!=(ColumnBase a, ColumnBase b) { return !(a == b); }

  private:
   friend class Row;

   std::size_t index() const { return index_; }

   // TODO: Add an argument for the enum value for the type.
   explicit ColumnBase(std::string name, std::size_t index)
       : name_(std::move(name)), index_(index) {}

   std::string name_;
   std::size_t index_;
   // some Type enum to implement is<T>()
};

template <typename T>
struct Column : ColumnBase {
  using type = T;
};

class ColumnRange {
 public:
  using iterator = std::vector<ColumnBase>::iterator;

  iterator begin() { return columns_.begin(); }
  iterator end() { return columns_.end(); }

  std::size_t size() const { return columns_.size(); }
  bool empty() const { return columns_.empty(); }

  StatusOr<ColumnBase> get(std::string name) const { return {}; }
  StatusOr<ColumnBase> get(std::size_t index) const { return {}; }

  template <typename T>
  StatusOr<Column<T>> get(std::string name) const {
    return {};
  }
  template <typename T>
  StatusOr<Column<T>> get(std::size_t index) const {
    return {};
  }

 private:
  std::vector<ColumnBase> columns_;
  // Maybe a map to make lookup by name faster enough?
};

// PICKUP!!!!!!!!!!!!!!!!!!!!!
// How can we use the new column stuff to make the 
// mutation factories nicer?
struct M {};
M MakeFooMutation(std::string table, ColumnRange cols) {
  return {};
}
M m = MakeFooMutation("MyTable", ColumnRange{});

// Represents a range of Values.
class Row {
 public:
  using iterator = std::vector<Value>::iterator;

  iterator begin() { return v_.begin(); }
  iterator end() { return v_.end(); }

  template <typename T>
  std::optional<T> get(ColumnBase c) {
    return v_[c.index()].get<T>();
  }

  // Overload that deduces T given a Column<T> argument.
  template <typename T>
  std::optional<T> get(Column<T> c) {
    return get<T>(ColumnBase(c));
  }

 private:
  friend class Client;
  void AddValue(Value value) { v_.push_back(std::move(value)); }
  std::vector<Value> v_;
};

class RowStream {
 public:
  using value_type = StatusOr<Row>;
  using iterator = std::vector<value_type>::iterator;

  iterator begin() { return {}; }
  iterator end() { return {}; }
};


// Represents a range of values that correspond to a DB table index. Similar to
// a Row (both contain a range of values), but this doesn't have column names,
// nor any of the accessors. Maybe we want to have some other accessors here?
//
// TODO: Need some way to specify a null value
class Key {
 public:
  template <typename... Ts>  // XXX: Add appropriate enablers.
  explicit Key(Ts&&... ts) : v{Value(std::forward<Ts>(ts))...} {}

 private:
  std::vector<Value> v;
};

// Represents the name of a database table index, along with a bunch of Keys
// and (TODO) key ranges for the specified index.
//
// TODO: Add support for key ranges as well.
// TODO: Do we want some fancy syntax to build key sets and ranges nicely?
//       Maybe op+, op+=, etc?
class KeySet {
 public:
  static KeySet All() { return KeySet(all_tag{}); }

  KeySet() = default;  // uses the primary index.
  explicit KeySet(std::string index_name) : index_(std::move(index_name)) {}
  KeySet(std::string index_name, std::vector<Key> keys)
      : index_(std::move(index_name)), keys_(std::move(keys)) {}

  void Add(Key key) { keys_.push_back(std::move(key)); }
  void Limit(int limit) { limit_ = limit; }

 private:
  struct all_tag {};
  explicit KeySet(all_tag) : all_(true) {}
  std::string index_;
  std::vector<Key> keys_;
  bool all_ = false;
  std::optional<int> limit_;
};

class ResultStats {
  // XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXxxxxxx
};

// Represents the result of a Spanner query/read RPC call. Gives access to the
// returned columns, rows, and stats.
// This will be returned from the Client::Read() function.
class ResultSet {
 public:
  ResultSet() = default;
  explicit ResultSet(std::vector<Row> v) {
    // ...
  }

  // XXX: perhaps moved ColumnRange into this class, and make this class itself
  // able to iterate the rows. 

  ColumnRange columns() const { return {}; }
  RowStream rows() const { return {}; }

  // XXX: Can only be called after consuming the whole stream.
  std::optional<ResultStats> stats() const { return {}; }

  // XXX: add the fetched transaction timestamp (if available)
};

// Represents an SQL statement with optional parameters. Parameter placeholders
// may be specified in the sql string using `@` followed by the parameter name.
// ... follow Spanner's docs about this.
class SqlStatement {
 public:
  // XXX: Can/should we re-use Value here? It could be made more generic so
  // that it's column attribute is just a generic "name", not necessarily a
  // column?
  using param_type = std::map<std::string, Value>;
  explicit SqlStatement(std::string sql, param_type params)
      : sql_(std::move(sql)), params_(std::move(params)) {}

  std::string sql() const { return sql_; }
  param_type params() const { return params_; }

 private:
  std::string sql_;
  param_type params_;
};

class Transaction {
 public:
  struct ReadOnlyOptions {
    // ...
  };
  struct ReadWriteOptions {
    // ...
  };
  // Value type.
  // No default c'tor, but defaulted copy, assign, move, etc.

  friend bool operator==(Transaction const& a, Transaction const& b) {
    return a.impl_ == b.impl_;
  }
  friend bool operator!=(Transaction const& a, Transaction const& b) {
    return !(a == b);
  }

 private:
  friend class Client;
  friend StatusOr<Transaction> DeserializeTransaction(std::string);
  friend std::string SerializeTransaction(Transaction);
  friend Transaction MakeReadOnlyTransaction(Transaction::ReadOnlyOptions);
  friend Transaction MakeReadWriteTransaction(Transaction::ReadWriteOptions);

  struct PartitionedDml {};
  explicit Transaction(PartitionedDml) {}
  static Transaction MakePartitionedDmlTransaction() {
    return Transaction(PartitionedDml{});
  }

  struct SingleUse {};
  explicit Transaction(SingleUse){};
  static Transaction MakeSingleUseTransaction() {
    return Transaction(SingleUse{});
  }

  explicit Transaction(ReadOnlyOptions opts) {}
  explicit Transaction(ReadWriteOptions opts) {}


  class Impl;
  std::shared_ptr<Impl> impl_;
};

Transaction MakeReadOnlyTransaction(Transaction::ReadOnlyOptions opts = {}) {
  return Transaction(opts);
}

Transaction MakeReadWriteTransaction(Transaction::ReadWriteOptions opts = {}) {
  return Transaction(opts);
}

std::string SerializeTransaction(Transaction tx) {
  // TODO: Use proto or something better to serialize
  return {};
}

StatusOr<Transaction> DeserializeTransaction(std::string s) {
  // TODO: Properly deserialize.
  return Transaction(Transaction::ReadOnlyOptions{});
}

class Mutation {
 public:
  // value type, ==, !=

 private:
  // Holds a google::spanner::v1::Mutation proto
};

Mutation MakeInsertMutation() { return {}; }
Mutation MakeUpdateMutation() { return {}; }
Mutation MakeInsertOrUpdateMutation() { return {}; }
Mutation MakeReplaceMutation() { return {}; }
Mutation MakeDeleteMutation() { return {}; }

// The options passed to PartitionRead() and PartitionQuery().
struct PartitionOptions {
  int64_t partition_size_bytes;  // currently ignored
  int64_t max_partitions;        // currently ignored
};

class ReadPartition {
 public:
  // Value type.
 private:
  friend std::string SerializeReadPartition(ReadPartition p);
  friend StatusOr<ReadPartition> DeserializeReadPartition(std::string s);
  std::string token_;
  Transaction tx_;
  std::string table_;
  KeySet keys_;
  std::vector<std::string> columns_;
};

std::string SerializeReadPartition(ReadPartition p) { return p.token_; }
StatusOr<ReadPartition> DeserializeReadPartition(std::string s) { return {}; }

class SqlPartition {
 public:
  // Value type.
 private:
  friend std::string SerializeSqlPartition(SqlPartition p);
  friend StatusOr<SqlPartition> DeserializeSqlPartition(std::string s);
  std::string token_;
  Transaction tx_;
  SqlStatement statement_;
};

std::string SerializeSqlPartition(SqlPartition p) { return p.token_; }
StatusOr<SqlPartition> DeserializeSqlPartition(std::string s) { return {}; }

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
  ResultSet Read(Transaction tx, std::string table, KeySet keys,
                    std::vector<std::string> columns) {
    // Fills in two rows of dummy data.
    std::vector<Row> v;
    int64_t data = 1;
    v.push_back(Row{});
    for (auto const& c : columns) {
      v.back().AddValue(Value(data++));
    }
    v.push_back(Row{});
    for (auto const& c : columns) {
      v.back().AddValue(Value(data++));
    }
    return ResultSet(v);
  }

  // Same as Read() above, but implicitly uses a single-use Transaction.
  ResultSet Read(std::string table, KeySet keys,
                    std::vector<std::string> columns) {
    auto single_use = Transaction::MakeSingleUseTransaction();
    return Read(std::move(single_use), std::move(table), std::move(keys),
                std::move(columns));
  }

  ResultSet Read(ReadPartition partition) {
    // TODO: Call Spanner's StreamingRead RPC with the data in `partition`.
    return {};
  }

  // NOTE: Requires a read-only transaction
  StatusOr<std::vector<ReadPartition>> PartitionRead(
      Transaction tx, std::string table, KeySet keys,
      std::vector<std::string> columns, PartitionOptions opts = {}) {
    // TODO: Call Spanner's PartitionRead() RPC.
    return {};
  }

  //
  // SQL methods
  //

  // TODO: Add support for QueryMode
  ResultSet ExecuteSql(Transaction tx, SqlStatement statement) {
    auto columns = {"col1", "col2", "col3"};
    // Fills in two rows of dummy data.
    std::vector<Row> v;
    double data = 1;
    v.push_back(Row{});
    for (auto const& c : columns) {
      v.back().AddValue(Value(data));
      data += 1;
    }
    v.push_back(Row{});
    for (auto const& c : columns) {
      v.back().AddValue(Value(data));
      data += 1;
    }
    return ResultSet(v);
  }

  ResultSet ExecuteSql(SqlStatement statement) {
    auto single_use = Transaction::MakeSingleUseTransaction();
    return ExecuteSql(std::move(single_use), std::move(statement));
  }

  ResultSet ExecuteSql(SqlPartition partition) {
    // TODO: Call Spanner's StreamingExecuteSql RPC with the data in
    // `partition`.
    return {};
  }

  // NOTE: Requires a read-only transaction
  StatusOr<std::vector<SqlPartition>> PartitionQuery(
      Transaction tx, SqlStatement statement, PartitionOptions opts = {}) {
    // TODO: Call Spanner's PartitionQuery() RPC.
    return {};
  }

  //
  // DML methods
  //

  // Note: Does not support single-use transactions, so no overload for that.
  // Note: statements.size() == result.size()
  std::vector<StatusOr<ResultStats>> ExecuteBatchDml(
      Transaction tx, std::vector<SqlStatement> statements) {
    // TODO: Call spanner's ExecuteBatchDml RPC.
    return {};
  }


  StatusOr<int64_t> ExecutePartitionedDml(SqlStatement statement) {
    // TODO: Call ExecuteSql() with a PartitionedDmlTransaction
    Transaction dml = Transaction::MakePartitionedDmlTransaction();
    ResultSet r = ExecuteSql(dml, statement);
    // Look at the result set stats and return the "row_count_lower_bound
    return 42;
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
  std::string session;  // TODO: Get this from tx
  return Client(std::move(session), std::move(labels));
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

  spanner::Transaction tx = spanner::MakeReadOnlyTransaction();

  // Demonstrate serializing and deserializing a Transaction
  std::string data = spanner::SerializeTransaction(tx);
  StatusOr<spanner::Transaction> tx2 = spanner::DeserializeTransaction(data);
  if (!tx2) return 3;
  /* assert(tx == tx2); */
  std::cout << "Using serialized transaction: " << data << "\n";

  StatusOr<spanner::Client> sc2 = spanner::MakeClient(*tx2);

  /* std::cout << "\n# Using Client::Read()...\n"; */
  /* spanner::ResultSet result = sc->Read(tx, table, std::move(keys), columns); */
  /* spanner::ColumnRange cols = result.columns(); */
  /* StatusOr<spanner::Column<int64_t>> col_D = cols.get<int64_t>("D"); */
  /* StatusOr<spanner::ColumnBase> col_E = cols.get("E"); */
  /* if (!col_D || !col_E) { */
  /*   std::cerr << "Unexpected column columns\n"; */
  /*   return -1; */
  /* } */

  /* for (StatusOr<spanner::Row>& row : result.rows()) { */
  /*   if (!row) { */
  /*     std::cout << "Read failed\n"; */
  /*     continue;  // Or break? Can the next read succeed? */
  /*   } */
  /*   std::optional<int64_t> v; */
  /*   v = row->get(*col_D); */
  /*   if (v) { */
  /*     std::cout << *v; */
  /*   } else { */
  /*     std::cout << "null"; */
  /*   } */
  /*   std::cout << "\n"; */

  /*   v = row->get<int64_t>(*col_E); */
  /*   (v ? std::cout << *v : std::cout << "null") << "\n"; */
  /* } */

  std::cout << "\n# Using Client::Read()...\n";

  spanner::ResultSet result = sc->Read(tx, table, std::move(keys), columns);
  spanner::ColumnRange cols = result.columns();
  StatusOr<spanner::Column<std::string>> col_singer_id = cols.get<std::string>("SingerId");
  StatusOr<spanner::Column<std::string>> col_album_id = cols.get<std::string>("AlbumId");
  StatusOr<spanner::Column<std::string>> col_album_title = cols.get<std::string>("AlbumTitle");
  if (!col_singer_id || !col_album_id || !col_album_title) {
    std::cerr << "Unexpected column columns\n";
    return -1;
  }

  for (StatusOr<spanner::Row>& row : result.rows()) {
    if (!row) {
      std::cout << "Read failed\n";
      continue;  // Or break? Can the next read succeed?
    }
    std::cout << col_singer_id->name() << ": " << *row->get(*col_singer_id)
              << ", "
              << "AlbumId: " << *row->get(*col_album_id) << ", "
              << "AlbumTitle: " << *row->get(*col_album_title) << "\n";
  }


  /* result = sc->Read(tx, table, std::move(keys), columns); */
  /* cols = result.columns(); */
  /* for (StatusOr<spanner::Row>& row : result.rows()) { */
  /*   if (!row) { */
  /*     std::cout << "Read failed\n"; */
  /*     continue;  // Or break? Can the next read succeed? */
  /*   } */
  /*   for (spanner::ColumnBase c : cols) { */
  /*     auto x = row.get<int64_t>(cols); */

  /*   } */
  /*   // ... */
  /* } */



  // Uses Client::Read().
  std::cout << "\n# Using Client::Read()...\n";
  result = sc->Read(tx, table, std::move(keys), columns);
  for (StatusOr<spanner::Row>& row : result.rows()) {
    if (!row) {
      std::cout << "Read failed\n";
      continue;  // Or break? Can the next read succeed?
    }

    /* // You can access values via accessors on the Row. You can specify either */
    /* // the column name or the column's index. */
    /* /1* assert(row->is<int64_t>("D")); *1/ */
    /* std::optional<int64_t> d = row->get<int64_t>("D"); */
    /* std::cout << "D=" << d.value_or(-1) << "\n"; */

    /* /1* assert(row->is<int64_t>(3)); *1/ */
    /* d = row->get<int64_t>(3); */
    /* std::cout << "D(index 3)=" << d.value_or(-1) << "\n"; */

    /* // Additionally, you can iterate all the Values in a Row. */
    /* std::cout << "Row:\n"; */
    /* for (spanner::Value& value : *row) { */
    /*   if (value.is<bool>()) { */
    /*     std::cout << "BOOL(" << *value.get<bool>() << ")\n"; */
    /*   } else if (value.is<int64_t>()) { */
    /*     std::cout << "INT64(" << value.get<int64_t>().value_or(-1) << ")\n"; */
    /*   } else if (value.is<double>()) { */
    /*     std::cout << "FLOAT64(" << *value.get<double>() << ")\n"; */
    /*   } */
    /*   // ... */
    /* } */
  }

  // Uses Client::ExecuteSql().
  std::cout << "\n# Using Client::ExecuteSql()...\n";
  spanner::SqlStatement sql(
      "select * from Mytable where id > @msg_id and name like @name",
      {{"msg_id", spanner::Value(int64_t{123})},
       {"name", spanner::Value(std::string("sally"))}});
  result = sc->ExecuteSql(tx, sql);
  for (StatusOr<spanner::Row>& row : result.rows()) {
    if (!row) {
      std::cout << "Read failed\n";
      continue;  // Or break? Can the next read succeed?
    }
    // ...
  }

  Status s = sc->Rollback(tx);
  // assert(s)
}

