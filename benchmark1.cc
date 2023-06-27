
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
std::string kDBPath = "/home/e4r/test-database/rocksdb-cpp";
#endif

const int kv_size =12;
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

void batch_write(DB* db,Options options,int batchSize,int no_kv_pairs){
   auto start = std::chrono::steady_clock::now();
    {
    int pair_count = 1;
    do {
           int batchKVPairs = 0;
           WriteBatch batch;
           while (pair_count<=no_kv_pairs && batchKVPairs < batchSize) {
                char fixed_length_key[kv_size];
                char fixed_length_value[kv_size];
                copy_str_to_fixed_length_arr("k" + std::to_string(pair_count), fixed_length_key, kv_size);
                copy_str_to_fixed_length_arr("v" + std::to_string(pair_count), fixed_length_value, kv_size);

                batch.Put(rocksdb::Slice(fixed_length_key, kv_size), rocksdb::Slice(fixed_length_value, kv_size));
                pair_count++;
                batchKVPairs++;
           }
           db->Write(WriteOptions(), &batch);
    } while (pair_count<=no_kv_pairs);

    }
    auto end = std::chrono::steady_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Elapsed time for batch write of batch size "<< batchSize << " for " << no_kv_pairs<< " pairs :"  << duration.count()
              << " milliseconds" << std::endl;
    rocksdb::DestroyDB(db->GetName(), options);
}

int main() {
  DB* db;
  Options options;
  rocksdb::FlushOptions flushOptions;
  flushOptions.wait = true;

  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  const int no_kv_pairs = 10000000;
  //Batch Write 1k
  {
  const int batchSize= 1000;
  batch_write(db, options, batchSize, no_kv_pairs);
  }
  //Batch Write 10k
  {
  const int batchSize= 10000;
  batch_write(db, options, batchSize, no_kv_pairs);
  }
  //Write
  {
      auto start = std::chrono::steady_clock::now();
      {
        for (int i = 1; i <= no_kv_pairs; i++) {
          char fixed_length_key[kv_size];
          char fixed_length_value[kv_size];
          copy_str_to_fixed_length_arr("k" + std::to_string(i), fixed_length_key, kv_size);
          copy_str_to_fixed_length_arr("v" + std::to_string(i), fixed_length_value, kv_size);

          db->Put(WriteOptions(),rocksdb::Slice(fixed_length_key, kv_size), rocksdb::Slice(fixed_length_value, kv_size));
        }
      }
      auto end = std::chrono::steady_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
      std::cout << "Elapsed time for write: " << duration.count()
                << " milliseconds" << std::endl;
    }

//Range Query
  {
        auto start = std::chrono::steady_clock::now();

        char minKey[kv_size];
        char maxKey[kv_size];
        copy_str_to_fixed_length_arr("k" + std::to_string(1), minKey, kv_size);
        copy_str_to_fixed_length_arr("k" + std::to_string(no_kv_pairs), maxKey, kv_size);

        std::string minKeyString(minKey, kv_size);
        std::string maxKeyString(maxKey, kv_size);

        rocksdb::Iterator* it = db->NewIterator(ReadOptions());
        it->Seek(rocksdb::Slice(minKeyString));

        for (; it->Valid() && it->key().ToString() <= maxKeyString; it->Next()) {
            std::string value = it->value().ToString();
        }

        auto end = std::chrono::steady_clock::now();
        auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "Elapsed time for range query: " << duration.count()
                 << " milliseconds" << std::endl;
  }

//Read
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
  //Batch Read
  {
      std::vector<rocksdb::Slice> keys;
      std::vector<std::string> values;

      auto start = std::chrono::steady_clock::now();
      {
        for (int i = 1; i <= no_kv_pairs; i++) {
          char fixed_length_key[kv_size];
          copy_str_to_fixed_length_arr("k" + std::to_string(i), fixed_length_key, kv_size);
          keys.push_back(rocksdb::Slice(fixed_length_key));
        }
      }

      std::vector<rocksdb::Status> readStatus  = db->MultiGet(ReadOptions(), keys, &values);
      auto end = std::chrono::steady_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
      std::cout << "Elapsed time for batch read: " << duration.count()
                << " milliseconds" << std::endl;
  }
  db->Close();
  rocksdb::DestroyDB(db->GetName(), options);
  delete db;

  return 0;
}

