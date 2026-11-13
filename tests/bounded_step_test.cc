#include "bounded_step.h"
#include <gtest/gtest.h>

TEST(BoundedStep, ClimbsFiveToFiftySevenInSixUpdates) {
  int32_t limit = 5;
  int updates = 0;
  while (limit != 57) {
    limit = bounded_step(limit, 57);
    updates++;
  }
  EXPECT_EQ(updates, 6);
}

TEST(BoundedStep, LimitOfOneCanGrow) { EXPECT_EQ(bounded_step(1, 100), 2); }

TEST(BoundedStep, DropsAtMostHalf) { EXPECT_EQ(bounded_step(10, 1), 5); }

TEST(BoundedStep, TargetInsideBoundsPassesThrough) {
  EXPECT_EQ(bounded_step(10, 12), 12);
  EXPECT_EQ(bounded_step(10, 7), 7);
}
