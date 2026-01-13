#include <bits/stdc++.h>
#include "rdtsc.h"
#include "cpupin.h"
#include "../SPSCQueue.h"

struct Msg
{
  uint64_t ts;
  int seq;
};

typedef SPSCQueue<Msg, 1 << 10> MsgQ;

const int loop = 1000000;  // 1M messages
const int sleep_cycles = 0;  // NO throttling - max throughput!
MsgQ _q;
std::atomic<bool> ready{false};

void sendthread() {
  if (!cpupin(6)) {
    exit(1);
  }

  MsgQ* q = &_q;

  while (!ready.load()) ;

  for (int i = 0; i < loop; i++) {
    Msg* msg = q->alloc();
    while (!msg) {
      msg = q->alloc();  // Busy wait nếu queue full
    }
    msg->seq = i;
    msg->ts = rdtscp();  // Timestamp NGAY TRƯỚC push
    q->push();

    // Throttle một chút
    auto expire = rdtsc() + sleep_cycles;
    while (rdtsc() < expire);
  }
}

void recvthread() {
  if (!cpupin(7)) {
    exit(1);
  }

  // Calibrate rdtscp overhead
  auto before = rdtscp();
  for (int i = 0; i < 99; i++) rdtscp();
  auto after = rdtscp();
  auto rdtscp_lat = (after - before) / 100;

  std::vector<uint64_t> lat_vec;
  lat_vec.reserve(loop);

  std::vector<std::pair<int, uint64_t>> outliers;

  MsgQ* q = &_q;

  while (!ready.load()) ;

  int cnt = 0;
  uint64_t sum_lat = 0;
  uint64_t max_lat = 0;

  while (cnt < loop) {
    Msg* msg = q->front();
    if (!msg) continue;

    uint64_t now = rdtscp();
    uint64_t delta = now - msg->ts;

    lat_vec.push_back(delta);
    sum_lat += delta;

    if (delta > max_lat) {
      max_lat = delta;
    }

    // Lưu các outliers > 10000 cycles
    if (delta > 10000) {
      outliers.push_back({msg->seq, delta});
    }

    q->pop();
    cnt++;
  }

  std::cout << "\n=== Latency Analysis (With Throttling) ===\n";
  std::cout << "Total messages: " << cnt << std::endl;
  std::cout << "RDTSCP overhead: " << rdtscp_lat << " cycles\n";
  std::cout << "Average latency: " << (sum_lat / cnt) << " cycles ("
            << ((sum_lat / cnt) * 1000.0 / 2300) << " ns @ 2.3GHz)\n";
  std::cout << "Max latency: " << max_lat << " cycles ("
            << (max_lat * 1000.0 / 2300) << " ns)\n";

  std::sort(lat_vec.begin(), lat_vec.end());

  std::cout << "\n=== Percentiles ===\n";
  auto print_pct = [&](const char* label, size_t idx) {
    uint64_t cycles = lat_vec[idx];
    double ns = cycles * 1000.0 / 2300;
    std::cout << label << ": " << std::setw(8) << cycles << " cycles ("
              << std::setw(8) << std::fixed << std::setprecision(2) << ns << " ns)\n";
  };

  print_pct("p50   ", lat_vec.size() / 2);
  print_pct("p90   ", (lat_vec.size() * 90) / 100);
  print_pct("p99   ", (lat_vec.size() * 99) / 100);
  print_pct("p99.9 ", (lat_vec.size() * 999) / 1000);
  print_pct("p99.99", (lat_vec.size() * 9999) / 10000);
  print_pct("worst ", lat_vec.size() - 1);

  // Phân tích outliers
  std::cout << "\n=== Outlier Analysis (>10000 cycles / >4.3us) ===\n";
  std::cout << "Number of outliers: " << outliers.size()
            << " (" << std::fixed << std::setprecision(3)
            << (100.0 * outliers.size() / cnt) << "%)\n";

  if (outliers.size() > 0 && outliers.size() < 50) {
    std::cout << "\nAll outliers:\n";
    std::sort(outliers.begin(), outliers.end(),
              [](auto& a, auto& b) { return a.second > b.second; });

    for (size_t i = 0; i < outliers.size(); i++) {
      std::cout << "  #" << std::setw(7) << outliers[i].first
                << ": " << std::setw(10) << outliers[i].second << " cycles ("
                << std::setw(8) << std::fixed << std::setprecision(2)
                << (outliers[i].second * 1000.0 / 2300) << " ns)\n";
    }
  } else if (outliers.size() >= 50) {
    std::cout << "\nTop 20 worst outliers:\n";
    std::sort(outliers.begin(), outliers.end(),
              [](auto& a, auto& b) { return a.second > b.second; });

    for (int i = 0; i < 20; i++) {
      std::cout << "  #" << std::setw(7) << outliers[i].first
                << ": " << std::setw(10) << outliers[i].second << " cycles ("
                << std::setw(8) << std::fixed << std::setprecision(2)
                << (outliers[i].second * 1000.0 / 2300) << " ns)\n";
    }
  }

  // Phân tích latency buckets
  std::cout << "\n=== Latency Distribution ===\n";
  struct Bucket {
    std::string label;
    uint64_t min, max;
    int count = 0;
  };

  std::vector<Bucket> buckets = {
    {"< 200 cyc", 0, 200},
    {"200-500", 200, 500},
    {"500-1K", 500, 1000},
    {"1K-5K", 1000, 5000},
    {"5K-10K", 5000, 10000},
    {"10K-50K", 10000, 50000},
    {"50K-100K", 50000, 100000},
    {"> 100K", 100000, UINT64_MAX}
  };

  for (auto lat : lat_vec) {
    for (auto& b : buckets) {
      if (lat >= b.min && lat < b.max) {
        b.count++;
        break;
      }
    }
  }

  for (auto& b : buckets) {
    if (b.count > 0) {
      std::cout << std::setw(10) << b.label << ": "
                << std::setw(8) << b.count << " msgs ("
                << std::setw(6) << std::fixed << std::setprecision(2)
                << (100.0 * b.count / cnt) << "%)\n";
    }
  }
}

int main() {
  std::cout << "Starting latency analysis benchmark...\n";
  std::cout << "Queue size: " << (1 << 10) << std::endl;
  std::cout << "Messages: " << loop << std::endl;
  std::cout << "Producer throttling: " << sleep_cycles << " cycles\n";

  std::thread trecv(recvthread);
  std::thread tsend(sendthread);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;

  tsend.join();
  trecv.join();

  auto end = std::chrono::high_resolution_clock::now();
  double seconds = std::chrono::duration<double>(end - start).count();
  std::cout << "\nTotal runtime: " << seconds << " seconds\n";
  std::cout << "Throughput: " << (loop / seconds / 1e6) << " Mops/sec\n";

  return 0;
}
