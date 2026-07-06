// JSON serialization that preserves Infinity / -Infinity / NaN. These appear in
// real game records (e.g. an active player's `retiredYear` is Infinity), and a
// plain JSON.stringify would turn them into null and corrupt the data on the
// receiving device. We store changesets as strings in Firestore anyway (to
// dodge its nested-array restrictions), so this is the single choke point.

const INF = "__Infinity__";
const NEG_INF = "__-Infinity__";
const NAN = "__NaN__";

const replacer = (_key: string, value: unknown) => {
	if (typeof value === "number") {
		if (value === Infinity) {
			return INF;
		}
		if (value === -Infinity) {
			return NEG_INF;
		}
		if (Number.isNaN(value)) {
			return NAN;
		}
	}
	return value;
};

const reviver = (_key: string, value: unknown) => {
	if (value === INF) {
		return Infinity;
	}
	if (value === NEG_INF) {
		return -Infinity;
	}
	if (value === NAN) {
		return NaN;
	}
	return value;
};

export const serializeChangeset = (changeset: unknown): string =>
	JSON.stringify(changeset, replacer);

export const deserializeChangeset = (serialized: string): any =>
	JSON.parse(serialized, reviver);
