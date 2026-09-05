// A ROTATION TO START FROM, SO NOBODY HAS TO PLAN FORTY-EIGHT MINUTES BLANK.
//
// This is what the page shows before a team has taken control of its own
// rotation, and what it resets to. It is a plan a reasonable coach might draw
// up from the roster as it stands: starters open each half, the bench comes
// in around the middle of quarters, minutes follow ability and whatever
// playing-time settings are in force, and nobody is asked to play the whole
// game.
//
// It is built a minute at a time. Every player carries a target for the game
// and, at each minute, whoever is furthest behind his target relative to the
// time left is the one who should be out there. Two rules keep that from
// producing a plan that changes five men every minute: a player who just came
// in stays a few minutes, and a player who just sat rests a couple. That is
// the whole coaching model, and it draws rotations that look like rotations.

import {
	gridToRotation,
	type RotationStint,
} from "../../../common/rotation.ts";

export type RotationCandidate = {
	pid: number;
	// The sim's own ranking of who should play. Anything monotonic will do.
	value: number;
	ptModifier: number;
	injured: boolean;
};

// Shares of the game by rank, for a five-man floor and a forty-eight minute
// game: roughly a starter's thirty-four down to a twelfth man's three. Scaled
// to whatever game this league plays and however many are on the floor.
const SHARES = [34, 33, 31, 29, 27, 22, 18, 15, 12, 10, 6, 3];
const SHARES_TOTAL = SHARES.reduce((sum, x) => sum + x, 0);

// Once a player is on he stays at least this long, and once he sits he rests
// at least this long, in minutes. A plan drawn at finer grain than this reads
// as noise.
const MIN_STINT = 3;
const MIN_REST = 2;

// The edge in urgency a bench player needs over a man on the floor to take his
// place, as a fraction of the game left. Four minutes of need with a full game
// to go, one minute with a quarter to go.
const SWAP_MARGIN = 0.08;

export const rotationTargets = (
	candidates: readonly RotationCandidate[],
	{
		gameMinutes,
		numPlayersOnCourt,
	}: { gameMinutes: number; numPlayersOnCourt: number },
): Map<number, number> => {
	const eligible = candidates
		.filter((p) => !p.injured && p.ptModifier > 0)
		.sort((a, b) => b.value - a.value);

	const floor = gameMinutes * numPlayersOnCourt;
	// Nobody plays the whole game by default; a few minutes' rest is the least
	// a plan should give a starter.
	const cap = Math.max(1, gameMinutes - 4);

	const raw = new Map<number, number>();
	for (const [i, p] of eligible.entries()) {
		const share = SHARES[i] ?? 0;
		raw.set(p.pid, (share / SHARES_TOTAL) * floor * p.ptModifier);
	}

	// Playing-time settings push the total off the floor's minutes, so scale
	// everybody back onto it, capped, and repeat until the cap stops binding.
	for (let pass = 0; pass < 8; pass++) {
		const sum = [...raw.values()].reduce((s, x) => s + x, 0);
		if (sum <= 0) {
			break;
		}
		const scale = floor / sum;
		let capped = false;
		for (const [pid, minutes] of raw) {
			const scaled = Math.min(cap, minutes * scale);
			if (scaled < minutes * scale) {
				capped = true;
			}
			raw.set(pid, scaled);
		}
		if (!capped) {
			break;
		}
	}

	// Whole minutes that add up to exactly the floor's minutes, by largest
	// remainder. A grid cannot hold half a minute.
	const targets = new Map<number, number>();
	let assigned = 0;
	const remainders: { pid: number; remainder: number }[] = [];
	for (const [pid, minutes] of raw) {
		const whole = Math.floor(minutes);
		targets.set(pid, whole);
		assigned += whole;
		remainders.push({ pid, remainder: minutes - whole });
	}
	remainders.sort((a, b) => b.remainder - a.remainder);
	for (const { pid } of remainders) {
		if (assigned >= floor) {
			break;
		}
		if (targets.get(pid)! < cap) {
			targets.set(pid, targets.get(pid)! + 1);
			assigned += 1;
		}
	}

	return targets;
};

export const generateRotation = (
	candidates: readonly RotationCandidate[],
	{
		numPeriods,
		periodLength,
		numPlayersOnCourt,
	}: { numPeriods: number; periodLength: number; numPlayersOnCourt: number },
): RotationStint[] => {
	const gameMinutes = numPeriods * periodLength;
	const targets = rotationTargets(candidates, {
		gameMinutes,
		numPlayersOnCourt,
	});
	const valueByPid = new Map(candidates.map((p) => [p.pid, p.value]));

	const pids = [...targets.keys()];
	if (pids.length === 0) {
		return [];
	}

	const played = new Map(pids.map((pid) => [pid, 0]));
	const enteredAt = new Map<number, number>();
	const satAt = new Map<number, number>();
	const grid = new Map(
		pids.map((pid) => [
			pid,
			Array.from({ length: numPeriods }, () =>
				Array.from({ length: periodLength }, () => false),
			),
		]),
	);

	const need = (pid: number) => targets.get(pid)! - played.get(pid)!;
	// Tie-break on ability so the better man gets the nod when needs match.
	const byNeed = (a: number, b: number) =>
		need(b) - need(a) || valueByPid.get(b)! - valueByPid.get(a)!;

	let onCourt: number[] = [];
	const halftime = Math.floor(numPeriods / 2);

	for (let period = 0; period < numPeriods; period++) {
		for (let minute = 0; minute < periodLength; minute++) {
			const m = period * periodLength + minute;
			const left = gameMinutes - m;
			const urgency = (pid: number) => need(pid) / left;

			if (minute === 0 && (period === 0 || period === halftime)) {
				// Each half opens with the starters: the men with the biggest
				// share of the game, whatever they have played so far.
				onCourt = [...pids]
					.sort(
						(a, b) =>
							targets.get(b)! - targets.get(a)! ||
							valueByPid.get(b)! - valueByPid.get(a)!,
					)
					.slice(0, numPlayersOnCourt);
				for (const pid of onCourt) {
					enteredAt.set(pid, m);
				}
			} else {
				const rested = (pid: number) =>
					satAt.get(pid) === undefined || m - satAt.get(pid)! >= MIN_REST;
				const settled = (pid: number) =>
					enteredAt.get(pid) !== undefined &&
					m - enteredAt.get(pid)! < MIN_STINT;

				// Worst-placed man on the floor first.
				const floor = [...onCourt].sort((a, b) => urgency(a) - urgency(b));
				for (const p of floor) {
					const bench = pids
						.filter(
							(pid) => !onCourt.includes(pid) && rested(pid) && need(pid) > 0,
						)
						.sort((a, b) => urgency(b) - urgency(a));
					const c = bench[0];
					if (c === undefined) {
						break;
					}

					// A man who has his minutes comes off, and a man being clearly
					// outplayed for them comes off - but neither before he has
					// been out there long enough for the change to read as a
					// change rather than a twitch. A target overshot by a minute
					// or two is a rotation; a one-minute stint is not.
					if (settled(p)) {
						continue;
					}
					const done = need(p) <= 0;
					const outplayed = urgency(c) > urgency(p) + SWAP_MARGIN;
					if (done || outplayed) {
						onCourt[onCourt.indexOf(p)] = c;
						enteredAt.set(c, m);
						satAt.set(p, m);
					}
				}
			}

			// Whatever happened above, the floor is full: anybody short is made
			// up from the best of the rest, need or no need.
			if (onCourt.length < numPlayersOnCourt) {
				const fill = pids
					.filter((pid) => !onCourt.includes(pid))
					.sort((a, b) => valueByPid.get(b)! - valueByPid.get(a)!);
				while (onCourt.length < numPlayersOnCourt && fill.length > 0) {
					const pid = fill.shift()!;
					onCourt.push(pid);
					enteredAt.set(pid, m);
				}
			}

			for (const pid of onCourt) {
				played.set(pid, played.get(pid)! + 1);
				grid.get(pid)![period]![minute] = true;
			}
		}
	}

	return gridToRotation(grid, periodLength);
};
