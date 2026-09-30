/**
 * Cloudflare Worker for CDN Edge Auth (v2)
 * Validates signed playback JWTs (EdDSA) issued by the playback service
 * against its JWKS at PLAYBACK_JWKS_URL, then proxies to RustFS origin.
 *
 * URL contract (matches playback-service GetPlayerConfig):
 *   /{artifact_prefix}/hls/master.m3u8?token={playback-token}
 *   /{artifact_prefix}/thumbnails/thumbnail.webp?token={playback-token}
 *   /{artifact_prefix}/storyboard/storyboard.vtt?token={storyboard-token}
 *   /{artifact_prefix}/subtitles/en.vtt?token={playback-token}
 */

const JWKS_CACHE_TTL_MS = 5 * 60 * 1000;
let jwksCache = { keys: null, fetchedAt: 0 };

async function getJWKS(env) {
  const now = Date.now();
  if (jwksCache.keys && now - jwksCache.fetchedAt < JWKS_CACHE_TTL_MS) {
    return jwksCache.keys;
  }
  const response = await fetch(env.PLAYBACK_JWKS_URL, { cf: { cacheTtl: 300 } });
  if (!response.ok) {
    throw new Error(`JWKS fetch failed: ${response.status}`);
  }
  const jwks = await response.json();
  jwksCache = { keys: jwks.keys ?? [], fetchedAt: now };
  return jwksCache.keys;
}

function b64urlToBytes(s) {
  const normalized = s.replace(/-/g, '+').replace(/_/g, '/');
  const pad = normalized.length % 4 === 0 ? '' : '='.repeat(4 - (normalized.length % 4));
  const binary = atob(normalized + pad);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

/**
 * Verify an EdDSA (Ed25519) JWT. Web Crypto supports Ed25519 in modern runtimes.
 * Returns the decoded claims or throws.
 */
async function verifyPlaybackToken(token, env) {
  const parts = token.split('.');
  if (parts.length !== 3) throw new Error('malformed token');
  const [headerB64, payloadB64, signatureB64] = parts;

  const header = JSON.parse(new TextDecoder().decode(b64urlToBytes(headerB64)));
  if (header.alg !== 'EdDSA') throw new Error(`unexpected alg ${header.alg}`);

  const keys = await getJWKS(env);
  const kid = header.kid;
  const jwk = keys.find(k => k.kid === kid) ?? keys[0];
  if (!jwk) throw new Error('no matching jwk');

  const publicKey = await crypto.subtle.importKey(
    'jwk',
    jwk,
    { name: 'Ed25519' },
    false,
    ['verify'],
  );
  const valid = await crypto.subtle.verify(
    'Ed25519',
    publicKey,
    b64urlToBytes(signatureB64),
    new TextEncoder().encode(`${headerB64}.${payloadB64}`),
  );
  if (!valid) throw new Error('signature mismatch');

  const claims = JSON.parse(new TextDecoder().decode(b64urlToBytes(payloadB64)));
  if (claims.exp && claims.exp * 1000 < Date.now()) throw new Error('token expired');
  return claims;
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    const token = url.searchParams.get('token');

    const parts = path.split('/').filter(p => p.length > 0);
    if (parts.length < 3) {
      return new Response('Not Found', { status: 404 });
    }
    // parts: [asset_id, runs, run_id, hls|thumbnails|storyboard|subtitles, ...]
    const resourceType = parts[3];
    const isMedia = ['hls', 'thumbnails', 'storyboard', 'subtitles'].includes(resourceType);

    try {
      if (isMedia) {
        if (!token) {
          return new Response('Forbidden: playback token required', { status: 403 });
        }
        let claims;
        try {
          claims = await verifyPlaybackToken(token, env);
        } catch (err) {
          return new Response(`Forbidden: ${err.message}`, { status: 403 });
        }
        // Storyboard VTT references the sprite with #xywh fragments; those
        // sub-requests carry the storyboard purpose token.
        if (resourceType === 'storyboard' && claims.purpose !== 'storyboard' && claims.purpose !== 'playback') {
          return new Response('Forbidden: wrong token purpose', { status: 403 });
        }
      }

      const originURL = `${env.ORIGIN_URL}${path}`;
      const originRequest = new Request(originURL, {
        method: request.method,
        headers: request.headers,
        body: request.body,
      });

      const response = await fetch(originRequest);

      if (response.status === 200) {
        const cacheHeaders = new Headers(response.headers);
        // Playlists must be revalidated early; segments/artifacts cache longer.
        const isPlaylist = path.endsWith('.m3u8') || path.endsWith('.vtt');
        cacheHeaders.set('Cache-Control', isPlaylist
          ? 'public, max-age=60'
          : 'public, max-age=86400, immutable');
        return new Response(response.body, {
          status: response.status,
          statusText: response.statusText,
          headers: cacheHeaders,
        });
      }

      return response;
    } catch (err) {
      return new Response(`Error: ${err.message}`, { status: 500 });
    }
  },
};
