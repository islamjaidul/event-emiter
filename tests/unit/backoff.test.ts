import { nextDelayMs } from '../../src/backoff';

describe('nextDelayMs', () => {
  it('follows schedule and cap', () => {
    expect(nextDelayMs(1)).toBe(10_000);
    expect(nextDelayMs(2)).toBe(60_000);
    expect(nextDelayMs(3)).toBe(300_000);
    expect(nextDelayMs(4)).toBe(1_800_000);
    expect(nextDelayMs(5)).toBe(7_200_000);
    expect(nextDelayMs(6)).toBe(21_600_000);
    expect(nextDelayMs(20)).toBe(21_600_000);
  });
});
