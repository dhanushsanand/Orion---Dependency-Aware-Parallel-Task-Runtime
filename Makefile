CXX      ?= g++
CXXFLAGS ?= -std=c++23 -O2 -Wall -Wextra -Wpedantic -Wno-missing-field-initializers -pthread -Iinclude
LDFLAGS  ?= -pthread

SRC  := src/object_store.cpp src/worker_pool.cpp src/task_manager.cpp src/runtime.cpp \
        src/cluster/message.cpp src/cluster/membership.cpp src/cluster/scheduler.cpp \
        src/cluster/tcp_transport.cpp
OBJ  := $(SRC:.cpp=.o)
LIB  := liborion.a
TESTS := test_object_store test_dag test_cluster
EXAMPLES := local_dag cluster_demo
BINS := orion_node $(EXAMPLES)

# ---- optional gRPC backend ------------------------------------------------
# make WITH_GRPC=1   (needs protoc, grpc_cpp_plugin, libgrpc++, libprotobuf)
ifeq ($(WITH_GRPC),1)
  CXXFLAGS += -DORION_WITH_GRPC $(shell pkg-config --cflags grpc++ protobuf)
  LDFLAGS  += $(shell pkg-config --libs grpc++ protobuf)
  GEN  := gen/orion.pb.cc gen/orion.grpc.pb.cc
  SRC  += src/cluster/grpc_transport.cpp $(GEN)
  OBJ  := $(SRC:.cpp=.o)
  OBJ  := $(OBJ:.cc=.o)
  CXXFLAGS += -Igen
gen/orion.pb.cc gen/orion.grpc.pb.cc: proto/orion.proto
	mkdir -p gen
	protoc -Iproto --cpp_out=gen --grpc_out=gen --plugin=protoc-gen-grpc=$$(which grpc_cpp_plugin) $<
src/cluster/grpc_transport.o: $(GEN)
endif

all: $(LIB) $(BINS)

$(LIB): $(OBJ)
	ar rcs $@ $^

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@
%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

orion_node: tools/orion_node.cpp $(LIB)
	$(CXX) $(CXXFLAGS) $< $(LIB) $(LDFLAGS) -o $@
local_dag: examples/local_dag.cpp $(LIB)
	$(CXX) $(CXXFLAGS) $< $(LIB) $(LDFLAGS) -o $@
cluster_demo: examples/cluster_demo.cpp $(LIB)
	$(CXX) $(CXXFLAGS) $< $(LIB) $(LDFLAGS) -o $@
test_%: tests/test_%.cpp $(LIB)
	$(CXX) $(CXXFLAGS) $< $(LIB) $(LDFLAGS) -o $@

test: $(TESTS)
	@for t in $(TESTS); do ./$$t || exit 1; done

tsan: CXXFLAGS += -fsanitize=thread -g -O1
tsan: LDFLAGS += -fsanitize=thread
tsan: clean test

asan: CXXFLAGS += -fsanitize=address,undefined -g -O1
asan: LDFLAGS += -fsanitize=address,undefined
asan: clean test

clean:
	rm -rf $(OBJ) $(LIB) $(BINS) $(TESTS) gen src/cluster/grpc_transport.o

.PHONY: all test tsan asan clean
