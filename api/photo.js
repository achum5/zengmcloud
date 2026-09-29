// Serves a player photo from our own origin, so the Face Converter can draw it
// on a canvas and copy the sheet as one image. Most photo hosts send no CORS
// headers, which leaves a canvas unreadable; fetched here, server side, the
// same pixels come back with CORS allowed. Only images are passed through.
//
// Vercel runs any file in api/ as a serverless function at /api/<name>.

const MAX_BYTES = 5_000_000;

const blockedHost = (host) =>
	host === "localhost" ||
	host.endsWith(".local") ||
	host.endsWith(".internal") ||
	/^[\d.]+$/.test(host) ||
	host.includes(":");

export default async function handler(req, res) {
	let target;
	try {
		target = new URL(new URL(req.url, "http://x").searchParams.get("url"));
	} catch {
		res.statusCode = 400;
		res.end();
		return;
	}
	if (!/^https?:$/.test(target.protocol) || blockedHost(target.hostname)) {
		res.statusCode = 400;
		res.end();
		return;
	}

	let upstream;
	try {
		upstream = await fetch(target, {
			headers: {
				Accept: "image/avif,image/webp,image/png,image/jpeg,image/*",
				"User-Agent":
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
			},
			signal: AbortSignal.timeout(10_000),
		});
	} catch {
		res.statusCode = 502;
		res.end();
		return;
	}

	const type = upstream.headers.get("content-type") ?? "";
	if (!upstream.ok || !type.startsWith("image/")) {
		res.statusCode = upstream.ok ? 415 : upstream.status;
		res.end();
		return;
	}
	const body = Buffer.from(await upstream.arrayBuffer());
	if (body.length > MAX_BYTES) {
		res.statusCode = 413;
		res.end();
		return;
	}

	res.setHeader("Content-Type", type);
	res.setHeader("Access-Control-Allow-Origin", "*");
	// Cached at Vercel's edge too, so a photo is fetched from its host once,
	// not every time a sheet is copied.
	res.setHeader(
		"Cache-Control",
		"public, max-age=31536000, s-maxage=31536000, immutable",
	);
	res.end(body);
}
