
#include <chrono>
#include <cstdio>
#include <iostream>
#include <string>
#include <random>
#include <algorithm>

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

std::string kDBPath = "/home/e4r/test-database/rocksdb-java";

void print_char_array(char arr[], size_t arr_size) {
  for (int i = 0; i < arr_size; i++) {
    printf("%c,", arr[i]);
  }
  printf("\n");
}

void measure_thread_execution_time(const std::function<void()> &function, std::string operation)
{
  auto start = std::chrono::steady_clock::now();
  function();
  auto end = std::chrono::steady_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cout << "Elapsed time for " << operation << " :" << duration.count()
            << " milliseconds" << std::endl;
}

class WorkloadConfiguration {
public:
    static const int numOfDistinctSPPairs = 1000;
    static const int variance = 1;
    static const int spMatches = 10000;

    static WorkloadConfiguration& getWorkloadConfiguration() {
        static WorkloadConfiguration instance;
        return instance;
    }

private:
    WorkloadConfiguration() {}
};

std::vector<uint8_t> getEdgeSPPair(const std::vector<uint8_t>& spPair) {
    std::vector<uint8_t> edgeSPPair = spPair;
    for (int i = edgeSPPair.size() - 1; i >= 0; i--) {
        edgeSPPair[i]++;
        if (edgeSPPair[i] != 0) {
            break;
        }
    }
    return edgeSPPair;
}
std::vector<uint8_t> convertToByteArray(int value) {
  std::vector<uint8_t> result(8);
  memcpy(result.data(), &value, sizeof(int));
  std::reverse(result.begin(), result.end());
  return result;
}

std::vector<uint8_t> append(const std::vector<uint8_t>& arr1, const std::vector<uint8_t>& arr2) {
    std::vector<uint8_t> finalArr(arr1.size() + arr2.size());
    memcpy(finalArr.data(), arr1.data(), arr1.size());
    memcpy(finalArr.data() + arr1.size(), arr2.data(), arr2.size());
    return finalArr;
}
class TestUtils {
public:
    class RandomSPPairGenerator : public std::iterator<std::input_iterator_tag, std::vector<uint8_t>> {
    private:
        const WorkloadConfiguration& workloadConfiguration;
        int noOfSPPairsLeft;
        
    public:
        RandomSPPairGenerator(const WorkloadConfiguration& workloadConfiguration, int numOfPairsToGenerate)
            : workloadConfiguration(workloadConfiguration), noOfSPPairsLeft(numOfPairsToGenerate) {}

        bool hasNext() const {
            return noOfSPPairsLeft > 0;
        }

        std::vector<uint8_t> next() {
            if (!hasNext()) {
                throw std::runtime_error("no SPPairs left");
            }

            std::random_device rd;
            std::mt19937 generator(rd());
            std::uniform_int_distribution<int> distribution(1, workloadConfiguration.numOfDistinctSPPairs);

            int value = distribution(generator);
            std::vector<uint8_t> S = convertToByteArray(value);
            noOfSPPairsLeft--;
            return append(S, S);
        }
        
    };
};

int compareKeys(const rocksdb::Slice& key1, const rocksdb::Slice& key2) {
    if (key1.size() != key2.size()) {
        throw std::runtime_error("key1 and key2 are of different lengths!");
    }

    for (size_t i = 0; i < key1.size(); i++) {
        int cmp = static_cast<int>(key1[i]) - static_cast<int>(key2[i]);
        if (cmp != 0) {
            return cmp;
        }
    }

    return 0;
}

void print_uint8_t_vector(std::vector<uint8_t> vec){
    for(int i=0;i<vec.size();i++){
        std::cout<<static_cast<int>(vec.at(i))<<"  ";
    }
    std::cout<<std::endl;
}

void print_slice(rocksdb::Slice slice){
    for(int i=0;i<slice.size();i++){
        std::cout<<static_cast<int>(slice[i])<<"  ";
    }
    std::cout<<std::endl;
}


void rangeQuery(DB* db, const std::vector<uint8_t>& minKey,  const std::vector<uint8_t>& maxKey) {
    ReadOptions readOptions;
    readOptions.fill_cache = false; // Optional: Set cache behavior

    rocksdb::Iterator* iterator = db->NewIterator(readOptions);

    rocksdb::Slice minKeySlice(reinterpret_cast<const char*>(minKey.data()), minKey.size());
    rocksdb::Slice maxKeySlice(reinterpret_cast<const char*>(maxKey.data()), maxKey.size());

    iterator->Seek(minKeySlice);

    while (iterator->Valid()) {
        rocksdb::Slice currentKey = iterator->key();

        // if (compareKeys(key, minKey) >= 0 && compareKeys(key, maxKey) < 0) {
        if (compareKeys(currentKey, maxKeySlice) < 0) {
            rocksdb::Slice value = iterator->value();
            if (value.size() == 0) {
                // value = nullptr; // C++ does not have an equivalent to Java's null
            }
            // print_slice(currentKey);
        } else {
            break;
        }
        iterator->Next();
    }

    delete iterator;
}


int main() {
  DB* db;
  Options options;

  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  const int no_of_keys_to_read = 100000;
  
  auto read_func = [db]()
  {
    WorkloadConfiguration& workloadConfig = WorkloadConfiguration::getWorkloadConfiguration();
    TestUtils::RandomSPPairGenerator generator(workloadConfig, no_of_keys_to_read); 
    while (generator.hasNext()) {
      std::vector<uint8_t> spPair = generator.next();
    //   print_uint8_t_vector(spPair);
      std::vector<uint8_t> O = convertToByteArray(0);
      std::vector<uint8_t> FromKey = append(spPair, O);
      std::vector<uint8_t> ToKey = append(getEdgeSPPair(spPair), O);

    //   print_uint8_t_vector(FromKey);
    //   print_uint8_t_vector(ToKey);

      rangeQuery(db, FromKey, ToKey);
        // Do something with the generated pair
    }
  };
  measure_thread_execution_time(read_func, "Read");

  db->Close();
//   rocksdb::DestroyDB(db->GetName(), options);
  delete db;

  return 0;
}

