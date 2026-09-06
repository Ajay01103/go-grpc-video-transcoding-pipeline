/**
 * CloudFlare Worker for CDN Edge Auth
 * Deploy to CDN origin (e.g., cdn.you.com)
 * Validates signed playback tokens on video/thumbnail/storyboard requests
 */

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const token = url.searchParams.get('token');
    const path = url.pathname;

    // Determine policy from path pattern
    // /playback_id/hls/master.m3u8?token=...
    // /playback_id/thumbnails/thumbnail.webp?token=...
    // /playback_id/storyboard/storyboard.vtt?token=...

    const parts = path.split('/').filter(p => p.length > 0);
    if (parts.length < 2) {
      return new Response('Not Found', { status: 404 });
    }

    const playbackID = parts[0];
    const resourceType = parts[1]; // hls, thumbnails, storyboard, subtitles

    try {
      // For public playback IDs, allow all requests (you can add rate limiting here)
      // For signed playback IDs, validate token

      // In a real implementation, you'd:
      // 1. Query your playback service to get the policy (public vs signed)
      // 2. If signed, validate the token JWT against your public key
      // 3. If token is invalid/expired, return 403

      // Stub: allow all for now (add real JWT validation in production)
      if (!token && resourceType !== 'thumbnails') {
        // Thumbnails might not need tokens depending on policy
        // For this stub, allow all
      }

      // Proxy to origin (RustFS/Garage in this case)
      const originURL = `${env.ORIGIN_URL}${path}`;
      const originRequest = new Request(originURL, {
        method: request.method,
        headers: request.headers,
        body: request.body,
      });

      const response = await fetch(originRequest);
      
      // Cache successful responses for 24 hours
      if (response.status === 200) {
        const cacheHeaders = new Headers(response.headers);
        cacheHeaders.set('Cache-Control', 'public, max-age=86400');
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
