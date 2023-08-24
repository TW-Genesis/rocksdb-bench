
ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

ifneq ($(USE_RTTI), 1)
	CXXFLAGS += -fno-rtti
endif

DIRS=build/cpp
$(shell mkdir -p $(DIRS))

bench: test_read

test_read: test_read.cc
	$(CXX) $(CXXFLAGS) $@.cc -o./build/cpp/$@ /home/e4r/workspace/rocksdb/librocksdb.so -I/home/e4r/workspace/rocksdb/include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)


clean:
	rm -rf ./test_read
