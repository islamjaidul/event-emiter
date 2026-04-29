export interface DeliveryResult {
  readonly success: boolean;
  readonly statusCode: number;
}

export async function deliverHttp(url: string, payload: unknown, timeoutMs: number): Promise<DeliveryResult> {
  const controller = new AbortController();
  const timeout = setTimeout(() => {
    controller.abort();
  }, timeoutMs);
  timeout.unref();

  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    return {
      success: response.status >= 200 && response.status < 300,
      statusCode: response.status,
    };
  } catch {
    return { success: false, statusCode: 0 };
  } finally {
    clearTimeout(timeout);
  }
}
