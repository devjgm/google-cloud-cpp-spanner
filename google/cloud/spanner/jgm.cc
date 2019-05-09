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
// * spanner::DatabaseClient - Represents a connection to a Spanner database.
//                             This class only has a Read() method currently.
//                             This one Read() method implements both streaming
//                             and non-streaming reads.
// * spanner::ReadStream - A "stream" of spanner::Rows.
//
// Look at the main() function at the bottom to see what the usage looks like.
//
// NOTES:
// * This file relies on C++17 only because of std::variant (and its friends).
//   We could get this from Abseil, or implement it ourselves.
//
// * This is currently lacking a Transaction class; I plan to add it next.
//
#include <cassert>
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

using ValueVariant = std::variant<BOOL, INT64, FLOAT64, STRING, ARRAY, STRUCT>;
struct Value : ValueVariant {
  using ValueVariant::ValueVariant;
};
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
      : column_(std::move(c)), value_{std::optional<T>(std::forward<T>(v))} {}

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
  explicit KeySet(std::string index_name) : index_(std::move(index_name)) {}

  void Add(Key key) { keys.push_back(std::move(key)); }

 private:
  std::string index_;
  std::vector<Key> keys;
};

// Represents a stream of Row objects. Actually, a stream of StatusOr<Row>.
// This will be returned from the DatabaseClient::Read() function.
// TODO: Implement a real stream, not just a vector, like we have here.
class ReadStream {
 public:
  using value_type = StatusOr<Row>;
  using iterator = std::vector<value_type>::iterator;
  using const_iterator = std::vector<value_type>::const_iterator;

  explicit ReadStream(std::vector<Row> v) {
    for (auto&& e : v) v_.emplace_back(e);
  }

  iterator begin() { return v_.begin(); }
  iterator end() { return v_.end(); }
  const_iterator begin() const { return v_.begin(); }
  const_iterator end() const { return v_.end(); }

 private:
  std::vector<value_type> v_;
};

// Represents a connection to a Spanner database.
class DatabaseClient {
 public:
  // Reads the columns for the given keys from the specified table. Returns a
  // stream of Rows.
  // TODO: Add support for "limit" integer.
  ReadStream Read(std::string table, KeySet keys,
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
    return ReadStream(v);
  }

  // TODO: Implement other methods, like ExecuteSql()
};

}  // namespace spanner

int main() {
  spanner::DatabaseClient db;  // XXX: Use some factory to create instances.

  spanner::KeySet keys("index2");
  keys.Add(spanner::Key(1.0, true, "hello"));

  std::string const table = "MyTable";
  std::vector<std::string> const columns = {"A", "B", "C", "D", "E"};

  for (StatusOr<spanner::Row>& row : db.Read(table, std::move(keys), columns)) {
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
}

