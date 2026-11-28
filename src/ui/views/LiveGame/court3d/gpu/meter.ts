// THE FRAME RATE OVER THE PICTURE (see setting.ts): over the last couple of
// seconds, frames a second, how many came late, the time the page's own
// thread spent on each (the mean, and the slowest twentieth) and on each
// part of it (the game worked out - where everybody is, the camera, the
// clocks - then the men's poses and the drawing, or today's drawing), the
// time between frames off the page's own (the card, the browser), how long
// the card itself took over a frame now and then, the slowest frame and the
// part of it that took longest, the picture's size and - where the browser
// tells - the page's memory.

const SPAN_MS = 2000;
const EVERY_MS = 500;

export type Parts = Record<string, number>;
export type Meter = {
	at: number[];
	work: number[];
	parts: Parts[];
	shownAt: number;
};
export const makeMeter = (): Meter => ({
	at: [],
	work: [],
	parts: [],
	shownAt: 0,
});

export const meterTick = (
	m: Meter,
	el: HTMLElement | null,
	now: number,
	workMs: number,
	parts: Parts,
	{
		gpu,
		fps,
		w,
		h,
		card,
	}: {
		gpu: boolean | undefined;
		fps: number;
		w: number;
		h: number;
		// The card's own time on each part of a frame, now and then (see
		// GpuScene card).
		card?: Record<string, number>;
	},
) => {
	m.at.push(now);
	m.work.push(workMs);
	m.parts.push(parts);
	while (m.at.length > 0 && m.at[0]! < now - SPAN_MS) {
		m.at.shift();
		m.work.shift();
		m.parts.shift();
	}
	if (!el || now - m.shownAt < EVERY_MS || m.at.length < 2) {
		return;
	}
	m.shownAt = now;
	const span = (m.at.at(-1)! - m.at[0]!) / 1000;
	const rate = (m.at.length - 1) / span;
	const due = 1000 / fps;
	let late = 0;
	for (let i = 1; i < m.at.length; i++) {
		if (m.at[i]! - m.at[i - 1]! > due * 1.5) {
			late++;
		}
	}
	const n = m.work.length;
	const sorted = [...m.work].sort((a, b) => a - b);
	const mean = m.work.reduce((a, b) => a + b, 0) / n;
	const p95 = sorted[Math.floor(n * 0.95)] ?? mean;
	const each = Object.keys(parts).map((k) => {
		const sum = m.parts.reduce((a, p) => a + (p[k] ?? 0), 0);
		return `${k} ${(sum / n).toFixed(1)}`;
	});
	let worst = 0;
	for (let i = 1; i < n; i++) {
		if (m.work[i]! > m.work[worst]!) {
			worst = i;
		}
	}
	const wp = Object.entries(m.parts[worst] ?? {}).sort(
		(a, b) => b[1] - a[1],
	)[0];
	const mem = (performance as { memory?: { usedJSHeapSize: number } }).memory;
	el.textContent = [
		gpu === undefined ? "Today" : gpu ? "GPU" : "GPU: none, today's",
		`${Math.round(rate)} fps${fps < 60 ? ` (held to ${fps})` : ""}`,
		`${Math.round((late / (m.at.length - 1)) * 100)}% late`,
		`${mean.toFixed(1)} ms (${p95.toFixed(1)})`,
		// (Between frames, off the page's own time: the card, the browser.)
		`between ${Math.max(0, 1000 / rate - mean).toFixed(1)}`,
		...each,
		...(card === undefined ? [] : [cardText(card)]),
		`worst ${Math.round(m.work[worst]!)}${wp ? ` (${wp[0]} ${Math.round(wp[1])})` : ""}`,
		`${w}×${h}`,
		...(mem ? [`${Math.round(mem.usedJSHeapSize / 2 ** 20)} MB`] : []),
	].join(" · ");
};

// The card's time on a frame, and on the parts that took it longest.
const cardText = (card: Record<string, number>) => {
	const all = Object.values(card).reduce((a, b) => a + b, 0);
	const most = Object.entries(card)
		.filter(([, ms]) => ms >= 0.5)
		.sort((a, b) => b[1] - a[1])
		.slice(0, 3)
		.map(([k, ms]) => `${k} ${ms.toFixed(1)}`);
	return `card ${all.toFixed(1)}${most.length > 0 ? ` (${most.join(", ")})` : ""}`;
};
