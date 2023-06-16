
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

bench: benchmark1

benchmark1: benchmark1.cc
	$(CXX) $(CXXFLAGS) $@.cc -o./build/cpp/$@ /usr/local/lib/librocksdb.so.8.4 -I/usr/local/include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)


clean:
	rm -rf ./benchmark1


