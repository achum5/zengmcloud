// THE FRAME RATE OVER THE PICTURE (see setting.ts): over the last couple of
// seconds, frames a second, how many came late, the time the page's own
// thread spent on each (the mean, and the slowest twentieth), the
// picture's size and - where the browser tells - the page's memory.

const SPAN_MS = 2000;
const EVERY_MS = 500;

export type Meter = {
	at: number[];
	work: number[];
	shownAt: number;
};
export const makeMeter = (): Meter => ({ at: [], work: [], shownAt: 0 });

export const meterTick = (
	m: Meter,
	el: HTMLElement | null,
	now: number,
	workMs: number,
	{
		gpu,
		fps,
		w,
		h,
	}: { gpu: boolean | undefined; fps: number; w: number; h: number },
) => {
	m.at.push(now);
	m.work.push(workMs);
	while (m.at.length > 0 && m.at[0]! < now - SPAN_MS) {
		m.at.shift();
		m.work.shift();
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
	const sorted = [...m.work].sort((a, b) => a - b);
	const mean = m.work.reduce((a, b) => a + b, 0) / m.work.length;
	const p95 = sorted[Math.floor(sorted.length * 0.95)] ?? mean;
	const mem = (performance as { memory?: { usedJSHeapSize: number } }).memory;
	el.textContent = [
		gpu === undefined ? "Today" : gpu ? "GPU" : "GPU: none, today's",
		`${Math.round(rate)} fps${fps < 60 ? ` (held to ${fps})` : ""}`,
		`${Math.round((late / (m.at.length - 1)) * 100)}% late`,
		`${mean.toFixed(1)} ms (${p95.toFixed(1)})`,
		`${w}×${h}`,
		...(mem ? [`${Math.round(mem.usedJSHeapSize / 2 ** 20)} MB`] : []),
	].join(" · ");
};
