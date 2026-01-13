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

const int loop = 10000000;  // 10M để chạy nhanh hơn
MsgQ _q;

void sendthread() {
  if (!cpupin(6)) {
    exit(1);
  }

  MsgQ* q = &_q;

  for (int i = 0; i < loop; i++) {
    Msg* msg = q->alloc();
    while (!msg) {
      msg = q->alloc();
    }
    msg->seq = i;
    msg->ts = rdtscp();  // Timestamp ngay trước push
    q->push();
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

  std::vector<std::pair<int, uint64_t>> outliers; // seq, latency

  MsgQ* q = &_q;

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

    // Lưu lại các outliers > 10000 cycles
    if (delta > 10000) {
      outliers.push_back({msg->seq, delta});
    }

    q->pop();
    cnt++;
  }

  std::cout << "\n=== Latency Analysis ===\n";
  std::cout << "Total messages: " << cnt << std::endl;
  std::cout << "RDTSCP overhead: " << rdtscp_lat << " cycles\n";
  std::cout << "Average latency: " << (sum_lat / cnt) << " cycles\n";
  std::cout << "Max latency: " << max_lat << " cycles\n";

  std::sort(lat_vec.begin(), lat_vec.end());

  std::cout << "\n=== Percentiles (cycles) ===\n";
  std::cout << "p50:    " << lat_vec[lat_vec.size() / 2] << std::endl;
  std::cout << "p90:    " << lat_vec[(lat_vec.size() * 90) / 100] << std::endl;
  std::cout << "p99:    " << lat_vec[(lat_vec.size() * 99) / 100] << std::endl;
  std::cout << "p99.9:  " << lat_vec[(lat_vec.size() * 999) / 1000] << std::endl;
  std::cout << "p99.99: " << lat_vec[(lat_vec.size() * 9999) / 10000] << std::endl;
  std::cout << "worst:  " << lat_vec[lat_vec.size() - 1] << std::endl;

  // Phân tích outliers
  std::cout << "\n=== Outlier Analysis (>10000 cycles) ===\n";
  std::cout << "Number of outliers: " << outliers.size()
            << " (" << (100.0 * outliers.size() / cnt) << "%)\n";

  if (outliers.size() > 0) {
    std::cout << "\nTop 10 worst outliers:\n";
    std::sort(outliers.begin(), outliers.end(),
              [](auto& a, auto& b) { return a.second > b.second; });

    for (int i = 0; i < std::min(10, (int)outliers.size()); i++) {
      std::cout << "  Seq #" << outliers[i].first
                << ": " << outliers[i].second << " cycles";

      // Tính xem có pattern không (mỗi bao nhiêu message thì có spike)
      if (i > 0 && outliers[i].first > 0) {
        int gap = outliers[i-1].first - outliers[i].first;
        std::cout << " (gap: " << gap << ")";
      }
      std::cout << std::endl;
    }

    // Phân tích pattern của outliers
    if (outliers.size() > 1) {
      std::vector<int> gaps;
      for (size_t i = 1; i < outliers.size(); i++) {
        gaps.push_back(outliers[i-1].first - outliers[i].first);
      }
      std::sort(gaps.begin(), gaps.end());

      std::cout << "\nGap statistics between outliers:\n";
      std::cout << "  Min gap: " << gaps.front() << " messages\n";
      std::cout << "  Max gap: " << gaps.back() << " messages\n";
      std::cout << "  Median gap: " << gaps[gaps.size()/2] << " messages\n";
    }
  }

  // Phân tích latency buckets
  std::cout << "\n=== Latency Distribution ===\n";
  std::map<std::string, int> buckets;
  buckets["< 200"] = 0;
  buckets["200-500"] = 0;
  buckets["500-1K"] = 0;
  buckets["1K-5K"] = 0;
  buckets["5K-10K"] = 0;
  buckets["10K-50K"] = 0;
  buckets["50K-100K"] = 0;
  buckets["> 100K"] = 0;

  for (auto lat : lat_vec) {
    if (lat < 200) buckets["< 200"]++;
    else if (lat < 500) buckets["200-500"]++;
    else if (lat < 1000) buckets["500-1K"]++;
    else if (lat < 5000) buckets["1K-5K"]++;
    else if (lat < 10000) buckets["5K-10K"]++;
    else if (lat < 50000) buckets["10K-50K"]++;
    else if (lat < 100000) buckets["50K-100K"]++;
    else buckets["> 100K"]++;
  }

  for (auto& [range, count] : buckets) {
    if (count > 0) {
      std::cout << range << " cycles: " << count
                << " (" << (100.0 * count / cnt) << "%)\n";
    }
  }
}

int main() {
  std::cout << "Starting latency analysis benchmark...\n";
  std::cout << "Queue size: " << (1 << 10) << std::endl;
  std::cout << "Messages: " << loop << std::endl;

  std::thread trecv(recvthread);
  std::thread tsend(sendthread);

  tsend.join();
  trecv.join();

  return 0;
}
