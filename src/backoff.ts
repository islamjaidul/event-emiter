const STEPS_MS = [10_000, 60_000, 300_000, 1_800_000, 7_200_000] as const;
const MAX_DELAY_MS = 21_600_000;

export function nextDelayMs(attempt: number): number {
  if (attempt <= 0) {
    return STEPS_MS[0];
  }

  const idx = attempt - 1;
  const value = STEPS_MS[idx];
  if (typeof value === 'number') {
    return value;
  }

  return MAX_DELAY_MS;
}
