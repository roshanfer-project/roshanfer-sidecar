#include "metrics.h"
#include <gtest/gtest.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

TEST(Metrics, DumpHoldsNamesMethodsAndSamples) {
  metrics::init("unit thread 0", 4);
  const uint16_t a = metrics::id("E2E INGRESS svc");
  const uint16_t b = metrics::id("LIMIT GLOBAL T:T");
  EXPECT_EQ(a, metrics::id("E2E INGRESS svc")); // interning is idempotent
  metrics::MethodCache cache;
  const uint16_t m = cache.get("get");
  EXPECT_EQ(m, cache.get("get"));
  metrics::record(a, 10.5F, m, 100);
  metrics::record(a, 11.0F, m, 101);
  metrics::record(b, 7.0F, 0, 102);
  metrics::record(b, 8.0F, 0, 103);

  std::string out;
  metrics::dump(out);
  EXPECT_EQ(out, "# unit thread 0\n"
                 "# name 1 E2E INGRESS svc\n"
                 "# name 2 LIMIT GLOBAL T:T\n"
                 "# method 1 get\n"
                 "100 1 1 10.5\n"
                 "101 1 1 11\n"
                 "102 2 0 7\n"
                 "103 2 0 8\n");
}

TEST(MetricsDeathTest, FullBufferIsFatal) {
  EXPECT_DEATH(
      {
        metrics::init("unit thread 0", 4);
        const uint16_t a = metrics::id("x");
        for (int i = 0; i < 5; i++)
          metrics::record(a, 1.0F);
      },
      "buffer full");
}

TEST(Metrics, ServeWritesHttpResponseWithDump) {
  metrics::init("unit thread 0", 4);
  metrics::record(metrics::id("x"), 1.0F, 0, 5);
  std::string expected;
  metrics::dump(expected);

  int sv[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
  const std::string req = "GET /dump HTTP/1.1\r\nHost: x\r\n\r\n";
  ASSERT_EQ(write(sv[0], req.data(), req.size()),
            static_cast<ssize_t>(req.size()));
  metrics::serve(sv[1]); // closes sv[1]

  std::string reply;
  char buf[4096];
  ssize_t n;
  while ((n = read(sv[0], buf, sizeof(buf))) > 0)
    reply.append(buf, static_cast<size_t>(n));
  close(sv[0]);

  ASSERT_EQ(reply.rfind("HTTP/1.0 200", 0), 0U);
  const auto body = reply.find("\r\n\r\n");
  ASSERT_NE(body, std::string::npos);
  EXPECT_EQ(reply.substr(body + 4), expected);
}
