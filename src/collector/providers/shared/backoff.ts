const BASE_DELAY_MS = 1_000;
const MAX_DELAY_MS = 30_000;
const STABLE_CONNECTION_MS = 60_000;

export function backoffDelayMs(attempt: number): number {
  const exponential = Math.min(MAX_DELAY_MS, BASE_DELAY_MS * 2 ** attempt);
  return Math.floor(Math.random() * exponential);
}

export function createStableConnectionTimer(
  onStable: () => void,
): NodeJS.Timeout {
  return setTimeout(onStable, STABLE_CONNECTION_MS);
}
