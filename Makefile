CXX      = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra -pthread

all: gateway dummy_backend loadgen

gateway: src/main.cpp src/socket_core.cpp src/socket_core.h src/logger.h
	$(CXX) $(CXXFLAGS) -o gateway src/main.cpp src/socket_core.cpp

dummy_backend: src/dummy_backend.cpp
	$(CXX) $(CXXFLAGS) -o dummy_backend src/dummy_backend.cpp

loadgen: src/loadgen.cpp
	$(CXX) $(CXXFLAGS) -o loadgen src/loadgen.cpp

clean:
	rm -f gateway dummy_backend loadgen gateway.log

.PHONY: all clean
