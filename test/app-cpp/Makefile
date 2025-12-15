CXX = g++
CXXFLAGS = -O3 -std=c++20 -pthread

all: app.o

app.o: main.cc
	$(CXX) $(CXXFLAGS) -o app.o main.cc

clean:
	rm -f app.o
