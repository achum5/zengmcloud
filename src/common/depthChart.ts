// A basketball depth chart from the roster order: the starters, then the next
// group of the same size, then the one after that - each group put in spots 1
// to n (point guard to center, with 5 on the court) by where the players'
// ratings fit them, from posValue (0 for a point guard up to 4 for a center).
export const depthChart = <T extends { posValue: number }>(
	players: T[],
	numPlayersOnCourt: number,
	numGroups = 3,
): (T | undefined)[][] => {
	const n = numPlayersOnCourt;
	const groups: (T | undefined)[][] = [];
	for (let i = 0; i < numGroups && i * n < players.length; i++) {
		const group = players
			.slice(i * n, (i + 1) * n)
			.sort((a, b) => a.posValue - b.posValue);
		groups.push(spotted(group, n));
	}
	return groups;
};

// Players (in order from point guard to center) into n spots, keeping their
// order, each as near as can be to the spot their ratings fit. A group short of
// players leaves open the spots that fit none of them.
const spotted = <T extends { posValue: number }>(
	group: T[],
	n: number,
): (T | undefined)[] => {
	const k = group.length;
	const fit = (i: number) =>
		(Math.min(4, Math.max(0, group[i]!.posValue)) * (n - 1)) / 4;

	// cost[i][j]: the least total miss putting the first i players in the first
	// j spots.
	const cost: number[][] = [];
	for (let i = 0; i <= k; i++) {
		cost.push(new Array(n + 1).fill(i === 0 ? 0 : Infinity));
	}
	for (let i = 1; i <= k; i++) {
		for (let j = i; j <= n; j++) {
			cost[i]![j] = Math.min(
				cost[i]![j - 1]!,
				cost[i - 1]![j - 1]! + Math.abs(fit(i - 1) - (j - 1)),
			);
		}
	}

	const spots: (T | undefined)[] = new Array(n).fill(undefined);
	for (let i = k, j = n; i > 0; j--) {
		if (cost[i]![j] !== cost[i]![j - 1]) {
			spots[j - 1] = group[i - 1];
			i -= 1;
		}
	}
	return spots;
};
