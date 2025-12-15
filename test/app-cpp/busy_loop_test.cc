#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <vector>

void busyLoop(int repeat) {
  for (int i = 0; i < repeat; ++i) {
    for (int j = 0; j < 10000; ++j) {
      // Empty loop to simulate work, similar to Go implementation
      asm(""); // Prevent optimization
    }
  }
}

int main() {
  std::vector<int> test_values = {1, 10, 100, 140, 200, 300, 340, 400};

  std::cout << "Benchmark busyLoop results:" << std::endl;
  std::cout << std::setw(10) << "Repeat" << std::setw(20) << "Avg Time (us)"
            << std::endl;
  std::cout << std::string(50, '-') << std::endl;

  int repeat = 100;

  for (int r : test_values) {
    int64_t sum = 0;
    for (int i = 0; i < repeat; i++) {
      auto start = std::chrono::high_resolution_clock::now();
      busyLoop(r);
      auto end = std::chrono::high_resolution_clock::now();

      auto duration_us =
          std::chrono::duration_cast<std::chrono::microseconds>(end - start)
              .count();
      sum += duration_us;
    }
    double avg = (double)sum / repeat;
    std::cout << std::setw(10) << r << std::setw(20) << avg << std::endl;
  }

  return 0;
}
