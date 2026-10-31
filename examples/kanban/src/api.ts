// Server API helpers for the demo.

/**
 * The sync URL's HTTP origin, where the dev token endpoint lives:
 * ws://host:port/v1/sync -> http://host:port
 */
export function apiBase(syncUrl: string): string {
  return syncUrl.replace(/^ws/, "http").split("/v1/")[0] ?? "";
}

/** devToken fetches a token from the server's dev_mode-only endpoint. */
export async function devToken(syncUrl: string, uid: string): Promise<string> {
  const res = await fetch(`${apiBase(syncUrl)}/dev/token?uid=${encodeURIComponent(uid)}`);
  if (!res.ok) {
    throw new Error(`dev token endpoint returned ${res.status} — is dev_mode enabled?`);
  }
  const body = (await res.json()) as { token: string };
  return body.token;
}
