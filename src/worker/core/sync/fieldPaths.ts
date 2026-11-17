// WHAT AN EDIT CHANGED, FIELD BY FIELD.
//
// A changeset carries whole records, and whole records lose a race: a league
// mate's trade queued while the room simmed went out after the sim and its
// player and team-season rows - captured before the sim - landed over the
// days of stats, injuries and wins the sim had just written. Its whole
// records said "everything about this player is as I last saw it", when all
// the trade meant was "he plays for this team now".
//
// So an ordinary edit (not a timeline advance) also carries the paths it
// changed, worked out against the record as the edit first read it
// (changeTracker's bases). A device applying it sets only those paths on its
// own copy; a device holding its own edit to the same record layers that
// back on top. Whole-record behavior is the fallback whenever the paths can't
// be trusted: no base, too many paths, a structure that doesn't line up.

export type FieldPath = (string | number)[];

// Deeper than this, a change is taken whole from its ancestor at this depth:
// ["stats", 12, "pts"] is a path; a nested array inside it is set whole.
const MAX_DEPTH = 3;
// More paths than this and the edit is shipped as a whole record, as before.
const MAX_PATHS = 200;

const isObject = (x: unknown): x is Record<string | number, unknown> =>
	typeof x === "object" && x !== null;

export const deepEqual = (a: unknown, b: unknown): boolean => {
	if (a === b) {
		return true;
	}
	if (typeof a === "number" && typeof b === "number") {
		return Number.isNaN(a) && Number.isNaN(b);
	}
	if (!isObject(a) || !isObject(b)) {
		return false;
	}
	if (Array.isArray(a) !== Array.isArray(b)) {
		return false;
	}
	if (Array.isArray(a)) {
		const bArray = b as unknown as unknown[];
		if (a.length !== bArray.length) {
			return false;
		}
		for (let i = 0; i < a.length; i++) {
			if (!deepEqual(a[i], bArray[i])) {
				return false;
			}
		}
		return true;
	}
	const aKeys = Object.keys(a).filter((k) => a[k] !== undefined);
	const bKeys = Object.keys(b).filter((k) => b[k] !== undefined);
	if (aKeys.length !== bKeys.length) {
		return false;
	}
	for (const k of aKeys) {
		if (!deepEqual(a[k], b[k])) {
			return false;
		}
	}
	return true;
};

// The paths at which `value` differs from `base`, or undefined when the
// record should travel whole (the whole record changed, or too many paths).
export const diffPaths = (
	base: unknown,
	value: unknown,
): FieldPath[] | undefined => {
	const out: FieldPath[] = [];
	const walk = (b: unknown, v: unknown, prefix: FieldPath) => {
		if (out.length > MAX_PATHS || deepEqual(b, v)) {
			return;
		}
		if (
			prefix.length >= MAX_DEPTH ||
			!isObject(b) ||
			!isObject(v) ||
			Array.isArray(b) !== Array.isArray(v)
		) {
			out.push(prefix);
			return;
		}
		if (Array.isArray(v)) {
			const bArray = b as unknown as unknown[];
			// Shrunk: the array as a whole (an index path can't say "gone").
			if (v.length < bArray.length) {
				out.push(prefix);
				return;
			}
			for (let i = 0; i < bArray.length; i++) {
				walk(bArray[i], v[i], [...prefix, i]);
			}
			for (let i = bArray.length; i < v.length; i++) {
				out.push([...prefix, i]);
			}
			return;
		}
		const keys = new Set([...Object.keys(b), ...Object.keys(v)]);
		for (const k of keys) {
			walk(b[k], v[k], [...prefix, k]);
		}
	};
	walk(base, value, []);
	if (out.length > MAX_PATHS || out.some((path) => path.length === 0)) {
		return undefined;
	}
	return out;
};

const clone = <T>(x: T): T =>
	x === undefined ? x : (structuredClone(x) as T);

// `target` with each path set to `source`'s value there (removed where
// `source` has none). A path that doesn't line up with `target` (a parent
// missing or of another shape, an index past the end) takes the whole
// top-level field from `source` instead. Returns a new object; neither input
// is changed.
export const applyPaths = <T>(
	target: T,
	source: T,
	paths: readonly FieldPath[],
): T => {
	if (!isObject(target) || !isObject(source)) {
		return clone(source);
	}
	const out = structuredClone(target) as Record<string | number, any>;
	const src = source as Record<string | number, any>;
	for (const path of paths) {
		if (path.length === 0) {
			return clone(source);
		}
		const top = path[0]!;
		let t: any = out;
		let s: any = src;
		let fits = true;
		for (let i = 0; i < path.length - 1; i++) {
			const k = path[i]!;
			s = isObject(s) ? s[k] : undefined;
			const next = t[k];
			if (
				!isObject(next) ||
				!isObject(s) ||
				Array.isArray(next) !== Array.isArray(s)
			) {
				fits = false;
				break;
			}
			t = next;
		}
		const last = path.at(-1)!;
		if (fits && Array.isArray(t) && typeof last === "number") {
			// Past the end would leave a hole.
			fits = last <= t.length;
		}
		if (!fits) {
			if (Object.hasOwn(src, top)) {
				out[top] = clone(src[top]);
			} else {
				delete out[top];
			}
			continue;
		}
		if (isObject(s) && Object.hasOwn(s, last)) {
			t[last] = clone(s[last]);
		} else if (Array.isArray(t)) {
			// (An index the source lacks: the source array is shorter, which
			// diffPaths ships as the whole array - not reached in practice.)
			t[last as number] = undefined;
		} else {
			delete t[last];
		}
	}
	return out as T;
};
