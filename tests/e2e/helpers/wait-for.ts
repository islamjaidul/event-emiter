export async function waitFor(
  check: () => boolean | Promise<boolean>,
  timeoutMs = 30_000,
  intervalMs = 50,
): Promise<void> {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (await check()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(`waitFor timeout after ${timeoutMs}ms`);
}
