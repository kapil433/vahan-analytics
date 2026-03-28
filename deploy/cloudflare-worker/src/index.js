/**
 * Cloudflare Worker: proxy to Render (or any HTTPS origin) and add a secret
 * request header only the edge knows. The browser never sees the secret.
 *
 * Deploy: see ../../DEPLOY.md § "Cloudflare Worker — secret header".
 */
export default {
  async fetch(request, env) {
    const originHost = (env.ORIGIN_HOST || "vahan-intelligence-api.onrender.com").replace(
      /^https?:\/\//,
      "",
    );
    const incoming = new URL(request.url);
    const target = new URL(incoming.pathname + incoming.search + incoming.hash, `https://${originHost}`);

    const headers = new Headers(request.headers);
    headers.set("Host", originHost);

    const secret = env.EDGE_SHARED_SECRET;
    if (secret) {
      headers.set("X-Vahan-Edge-Secret", secret);
    }

    const init = {
      method: request.method,
      headers,
      redirect: "manual",
    };
    if (request.method !== "GET" && request.method !== "HEAD") {
      init.body = request.body;
    }

    return fetch(target, init);
  },
};
