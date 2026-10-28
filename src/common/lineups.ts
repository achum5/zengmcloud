// The lineups a box score shows: as many as this, of those that played at
// least this long together (less is noise).
export const MIN_LINEUP_SEC = 150;
export const MAX_LINEUPS = 10;

type Line = {
	pids: number[];
	sec: number;
	poss: number;
	possOpp: number;
	pts: number;
	ptsOpp: number;
};

const combinations = (pids: readonly number[], n: number): number[][] => {
	if (n === 0) {
		return [[]];
	}
	const out: number[][] = [];
	for (let i = 0; i + n <= pids.length; i++) {
		for (const rest of combinations(pids.slice(i + 1), n - 1)) {
			out.push([pids[i]!, ...rest]);
		}
	}
	return out;
};

// The groups of n inside the fives - each four, three or pair, added up over
// every five it was part of. Most minutes first.
export const groupsOf = (lines: readonly Line[], n: number): Line[] => {
	const byKey = new Map<string, Line>();
	for (const line of lines) {
		const groups =
			n >= line.pids.length ? [line.pids] : combinations(line.pids, n);
		for (const pids of groups) {
			const key = pids.join(",");
			let group = byKey.get(key);
			if (!group) {
				group = { pids, sec: 0, poss: 0, possOpp: 0, pts: 0, ptsOpp: 0 };
				byKey.set(key, group);
			}
			group.sec += line.sec;
			group.poss += line.poss;
			group.possOpp += line.possOpp;
			group.pts += line.pts;
			group.ptsOpp += line.ptsOpp;
		}
	}
	return [...byKey.values()].sort(
		(a, b) => b.sec - a.sec || b.poss + b.possOpp - (a.poss + a.possOpp),
	);
};
