// Time the GM's decisions take, by kind, and how many franchise values each
// worked out (the arena reports it: layer 3's speed work).
export const smartClock: Record<
	string,
	{ n: number; ms: number; values: number }
> = {};

// Franchise values worked out so far (not read from a cache).
export const valueCount = { n: 0 };

const record = (kind: string, started: number, values: number) => {
	const c = (smartClock[kind] ??= { n: 0, ms: 0, values: 0 });
	c.n += 1;
	c.ms += performance.now() - started;
	c.values += valueCount.n - values;
};

// A stretch of code timed by hand: mark() where it starts, lap() where it
// ends.
export const mark = () => ({
	started: performance.now(),
	values: valueCount.n,
});
export const lap = (kind: string, m: ReturnType<typeof mark>) =>
	record(kind, m.started, m.values);

export const timed = async <T>(kind: string, f: () => Promise<T>) => {
	const started = performance.now();
	const values = valueCount.n;
	try {
		return await f();
	} finally {
		record(kind, started, values);
	}
};

export const timedSync = <T>(kind: string, f: () => T) => {
	const started = performance.now();
	const values = valueCount.n;
	try {
		return f();
	} finally {
		record(kind, started, values);
	}
};
