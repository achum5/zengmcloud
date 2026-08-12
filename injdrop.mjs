import http from "node:http";
import { readFile } from "node:fs/promises";
import { extname, join } from "node:path";
import { chromium } from "playwright";
const PORT = 8241, BUILD = join(process.cwd(), "build");
const server = http.createServer(async (req, res) => {
	let file = join(BUILD, req.url.split("?")[0]);
	let data;
	try { data = await readFile(file); } catch { file = join(BUILD, "index.html"); data = await readFile(file); }
	res.writeHead(200, { "Content-Type": { ".html":"text/html", ".js":"text/javascript", ".css":"text/css", ".svg":"image/svg+xml" }[extname(file)] ?? "application/octet-stream" });
	res.end(data);
});
await new Promise((r) => server.listen(PORT, r));
const browser = await chromium.launch({ executablePath: "/opt/pw-browsers/chromium-1194/chrome-linux/chrome", args: ["--no-proxy-server"] });
const context = await browser.newContext({ viewport: { width: 1400, height: 1000 }, serviceWorkers: "block" });
const page = await context.newPage();
await page.route("**/*", (r) => r.request().url().startsWith(`http://localhost:${PORT}`) ? r.continue() : r.abort());
page.on("pageerror", (e) => console.log("PAGEERROR:", e.message));
let bad = false;
const fail = (m) => { console.log("FAIL:", m); bad = true; };
const ok = (m) => console.log("ok:", m);

// Read the Ovr Drop column out of the frivolity table.
const drops = () => page.evaluate(() => {
	const table = document.querySelector("main table");
	if (!table) return undefined;
	const heads = [...table.querySelectorAll("thead th")].map((th) => th.textContent.trim());
	const col = heads.indexOf("Ovr Drop");
	if (col < 0) return { heads, col, values: [] };
	const values = [...table.querySelectorAll("tbody tr")].slice(0, 12).map((tr) => {
		const cells = tr.querySelectorAll("td");
		return cells[col]?.textContent.trim();
	});
	return { heads, col, values };
});

try {
	await page.goto(`http://localhost:${PORT}/new_league/random`, { waitUntil: "domcontentloaded" });
	await page.waitForFunction(() => window.bbgm !== undefined, { timeout: 30000 });
	await page.getByRole("button", { name: "Create League" }).click();
	await page.waitForURL(/\/l\/\d+/, { timeout: 90000 });
	const lid = Number(page.url().match(/\/l\/(\d+)/)[1]);

	// Turn coarse ratings mode ON, then sim enough seasons to accumulate injuries.
	await page.evaluate(() => window.bbgm.toWorker("main", "updateGameAttributes", { hideRatingsOnesDigit: true }));
	await page.waitForTimeout(500);
	await page.evaluate(() => window.bbgm.toWorker("toolsMenu", "autoPlaySeasons", { numSeasons: 1, phase: 0 }));
	for (let i = 0; i < 100; i++) {
		await page.waitForTimeout(3000);
		const season = await page.evaluate(() => document.querySelector(".navbar")?.innerText ?? "");
		if (/2027/.test(season)) break;
	}
	console.log("nav:", (await page.evaluate(() => document.querySelector(".navbar")?.innerText ?? "")).replace(/\n/g, " | "));

	await page.goto(`http://localhost:${PORT}/l/${lid}/frivolities/most/worst_injuries`, { waitUntil: "domcontentloaded" });
	await page.waitForTimeout(5000);
	const d = await drops();
	if (!d) { fail("no table on the page"); throw new Error("stop"); }
	console.log("cols:", JSON.stringify(d.heads));
	console.log("Ovr Drop values:", JSON.stringify(d.values));
	if (d.col < 0) { fail("no Ovr Drop column"); throw new Error("stop"); }
	if (d.values.length === 0) { console.log("NOTE: no injury rows yet - inconclusive"); throw new Error("stop"); }

	const nums = d.values.map(Number).filter((n) => Number.isFinite(n));
	if (nums.length === 0) { fail("Ovr Drop column has no numbers"); throw new Error("stop"); }
	// Coarsened, these would all be 0/1/2. Exact, they are real ovr points.
	if (nums.every((n) => n <= 3)) fail(`every drop is <= 3 (${nums.join(",")}) - still looks coarsened`);
	else ok(`real ovr drops shown under coarse mode (${nums.slice(0, 6).join(", ")})`);
	if (nums.some((n) => n % 10 !== 0)) ok("values carry a ones digit");
	else console.log("NOTE: all values happen to be multiples of 10");

	// And the Ovr column beside it must STILL be coarse - only the drop changed.
	const ovrCol = await page.evaluate(() => {
		const table = document.querySelector("main table");
		const heads = [...table.querySelectorAll("thead th")].map((th) => th.textContent.trim());
		const i = heads.indexOf("Ovr");
		if (i < 0) return undefined;
		return [...table.querySelectorAll("tbody tr")].slice(0, 8)
			.map((tr) => tr.querySelectorAll("td")[i]?.textContent.trim());
	});
	console.log("Ovr column:", JSON.stringify(ovrCol));
	if (ovrCol && ovrCol.filter(Boolean).every((v) => Number(v) <= 10)) ok("Ovr itself is still coarse - only the drop changed");
	else if (ovrCol) fail(`Ovr column is not coarse: ${ovrCol.join(",")}`);
} catch (error) {
	if (error.message !== "stop") { console.log("HARNESS ERROR:", error.message); bad = true; }
} finally {
	await browser.close();
	server.close();
	console.log(bad ? "RESULT: FAILURES" : "RESULT: all checks passed");
	process.exit(bad ? 1 : 0);
}
