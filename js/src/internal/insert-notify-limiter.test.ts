import { describe, expect, it } from "vitest";

import { InsertNotifyLimiter } from "./insert-notify-limiter.js";

describe("InsertNotifyLimiter", () => {
  const setup = (cooldownMs = 100) => {
    const clock = { now: 1_000 };
    const limiter = new InsertNotifyLimiter(cooldownMs, () => clock.now);
    return { clock, limiter };
  };

  it("allows each queue once, in first-seen order", () => {
    const { limiter } = setup();

    expect(limiter.allow(["beta", "alpha", "beta"])).toEqual(["beta", "alpha"]);
    expect(limiter.allow([])).toEqual([]);
  });

  it("suppresses a queue through its cooldown, like River for Go", () => {
    const { clock, limiter } = setup();

    expect(limiter.allow(["alpha"])).toEqual(["alpha"]);
    clock.now += 50;
    expect(limiter.allow(["alpha", "beta"])).toEqual(["beta"]);
    // Go allows a queue only once its last notification is strictly older
    // than the cooldown.
    clock.now += 50;
    expect(limiter.allow(["alpha"])).toEqual([]);
    clock.now += 1;
    expect(limiter.allow(["alpha", "beta"])).toEqual(["alpha"]);
  });
});
