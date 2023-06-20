
#include <chrono>
#include <cstdio>
#include <iostream>
#include <string>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb-cpp";
#endif

void copy_str_to_fixed_length_arr(std::string str, char fixed_len_arr[],
                                  size_t arr_size) {
  for (int i = 0; i < arr_size; i++) {
    fixed_len_arr[i] = 0;
  }

  int i = arr_size - str.length();
  int j = 0;
  while (j < str.length()) {
    fixed_len_arr[i] = str[j];
    j++, i++;
  }
}

void print_char_array(char arr[], size_t arr_size) {
  for (int i = 0; i < arr_size; i++) {
    printf("%c,", arr[i]);
  }
  printf("\n");
}

std::string convert_to_string(char arr[], int size)
{
    int i;
    std::string s = "";
    for (i = 0; i < size; i++) {
        s = s + arr[i];
    }
    return s;
}

int main() {
  DB* db;
  Options options;
  rocksdb::FlushOptions flushOptions;
  flushOptions.wait = true;

  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
//   options.IncreaseParallelism();
//   options.OptimizeLevelStyleCompaction();

  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  std::string value;
  const int no_kv_pairs = 10000000;
  const int kv_size = 10;
  {
    auto start = std::chrono::steady_clock::now();
    {
      WriteBatch batch;
      // batch.Delete("key1");
      for (int i = 1; i <= no_kv_pairs; i++) {
        char fixed_length_key[kv_size];
        char fixed_length_value[kv_size];
        copy_str_to_fixed_length_arr("k" + std::to_string(i), fixed_length_key, kv_size);
        copy_str_to_fixed_length_arr("v" + std::to_string(i), fixed_length_value, kv_size);

         batch.Put(rocksdb::Slice(fixed_length_key, kv_size), rocksdb::Slice(fixed_length_value, kv_size));
//        s = db->Put(WriteOptions(), rocksdb::Slice(fixed_length_key, kv_size), rocksdb::Slice(fixed_length_value, kv_size));
      }
       s = db->Write(WriteOptions(), &batch);
      // db->Flush(flushOptions);
    }
    auto end = std::chrono::steady_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Elapsed time for write: " << duration.count()
              << " milliseconds" << std::endl;
  }

  {
    auto start = std::chrono::steady_clock::now();
    {
      for (int i = 1; i <= no_kv_pairs; i++) {
        char fixed_length_key[kv_size];
        std::string value;
        copy_str_to_fixed_length_arr("k" + std::to_string(i), fixed_length_key, kv_size);

        db->Get(ReadOptions(), rocksdb::Slice(fixed_length_key, kv_size), &value);
      }
    }
    auto end = std::chrono::steady_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Elapsed time for read: " << duration.count()
              << " milliseconds" << std::endl;
  }
  db->Close();
  rocksdb::DestroyDB(db->GetName(), options);
  delete db;

  return 0;
}

