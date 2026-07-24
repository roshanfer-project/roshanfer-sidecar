#include "ppm_queue.h"

#include <gtest/gtest.h>

TEST(FlexiblePriorityQueueTest, PopsAscendingPriority) {
  FlexiblePriorityQueue q;
  q.push(3, 30);
  q.push(1, 10);
  q.push(2, 20);

  EXPECT_EQ(q.size(), 3u);
  EXPECT_EQ(q.pop(), 1);
  EXPECT_EQ(q.pop(), 2);
  EXPECT_EQ(q.pop(), 3);
  EXPECT_EQ(q.size(), 0u);
}

TEST(FlexiblePriorityQueueTest, EqualPriorityTieBreakById) {
  FlexiblePriorityQueue q;
  q.push(5, 10);
  q.push(2, 10);
  q.push(9, 10);

  EXPECT_EQ(q.pop(), 2);
  EXPECT_EQ(q.pop(), 5);
  EXPECT_EQ(q.pop(), 9);
}
