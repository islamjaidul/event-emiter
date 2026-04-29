import { deliverHttp } from '../../src/infrastructure/http/delivery';

describe('deliverHttp', () => {
  it('returns success for 2xx', async () => {
    const spy = jest.spyOn(globalThis, 'fetch').mockResolvedValue({ status: 200 } as Response);
    const result = await deliverHttp('http://x.test', { a: 1 }, 1000);
    expect(result.success).toBe(true);
    spy.mockRestore();
  });

  it('returns failure for non-2xx', async () => {
    const spy = jest.spyOn(globalThis, 'fetch').mockResolvedValue({ status: 500 } as Response);
    const result = await deliverHttp('http://x.test', { a: 1 }, 1000);
    expect(result.success).toBe(false);
    spy.mockRestore();
  });

  it('returns failure on fetch throw', async () => {
    const spy = jest.spyOn(globalThis, 'fetch').mockRejectedValue(new Error('timeout'));
    const result = await deliverHttp('http://x.test', { a: 1 }, 1);
    expect(result.success).toBe(false);
    spy.mockRestore();
  });
});
