import { shuffle } from "../../../common/random.ts";

// College schedule: a non-conference stretch in November and December, then
// conference play. Small conferences play a double round robin, big ones play
// everybody once plus some rematches, capped at 20 conference games. The rest
// of a 31-game season is filled with non-conference games against teams from
// other conferences.

type ScheduleTeam = {
	tid: number;
	seasonAttrs: { cid: number };
};

const MAX_CONF_GAMES = 20;

export const confGamesTarget = (numTeams: number) =>
	Math.min(2 * (numTeams - 1), MAX_CONF_GAMES);

// Circle-method round robin. Odd team counts get a bye (-1) each round.
const roundRobin = (tids: number[]) => {
	const list = [...tids];
	if (list.length % 2 === 1) {
		list.push(-1);
	}
	const n = list.length;
	const rounds: [number, number][][] = [];
	for (let r = 0; r < n - 1; r++) {
		const round: [number, number][] = [];
		for (let i = 0; i < n / 2; i++) {
			const a = list[i]!;
			const b = list[n - 1 - i]!;
			if (a !== -1 && b !== -1) {
				// Alternate home and away so nobody is on the road every week.
				round.push(r % 2 === 0 ? [a, b] : [b, a]);
			}
		}
		rounds.push(round);
		// Rotate everyone but the first team.
		list.splice(1, 0, list.pop()!);
	}
	return rounds;
};

const newScheduleCollege = (teams: ScheduleTeam[], numGames: number) => {
	const byConf = new Map<number, number[]>();
	for (const t of teams) {
		const list = byConf.get(t.seasonAttrs.cid) ?? [];
		list.push(t.tid);
		byConf.set(t.seasonAttrs.cid, list);
	}
	const cidByTid = new Map(teams.map((t) => [t.tid, t.seasonAttrs.cid]));

	// Conference play, each conference's rounds lined up in parallel.
	const gamesPlayed = new Map(teams.map((t) => [t.tid, 0]));
	const homeGames = new Map(teams.map((t) => [t.tid, 0]));
	const confRounds: [number, number][][] = [];
	for (const tids of byConf.values()) {
		if (tids.length < 2) {
			continue;
		}
		const shuffled = [...tids];
		shuffle(shuffled);
		const single = roundRobin(shuffled);
		const target = confGamesTarget(tids.length);
		const gamesPerRound = (2 * Math.floor(tids.length / 2)) / tids.length;
		const numRounds = Math.round(target / gamesPerRound);
		for (let r = 0; r < numRounds; r++) {
			const pass = Math.floor(r / single.length);
			const base = single[r % single.length]!;
			// The second time through, the home team switches.
			const round = base.map(
				([a, b]) => (pass % 2 === 0 ? [a, b] : [b, a]) as [number, number],
			);
			const slot = (confRounds[r] ??= []);
			slot.push(...round);
			for (const [a, b] of round) {
				gamesPlayed.set(a, gamesPlayed.get(a)! + 1);
				gamesPlayed.set(b, gamesPlayed.get(b)! + 1);
				homeGames.set(a, homeGames.get(a)! + 1);
			}
		}
	}

	// Non-conference: random cross-conference pairings until everyone has a
	// full season.
	const needed = new Map(
		teams.map((t) => [t.tid, Math.max(0, numGames - gamesPlayed.get(t.tid)!)]),
	);
	const met = new Set<string>();
	const key = (a: number, b: number) => (a < b ? `${a}-${b}` : `${b}-${a}`);
	const nonConfRounds: [number, number][][] = [];
	for (let attempt = 0; attempt < numGames * 3; attempt++) {
		const pending = teams
			.map((t) => t.tid)
			.filter((tid) => needed.get(tid)! > 0);
		if (pending.length < 2) {
			break;
		}
		shuffle(pending);
		// Teams with the most games left go first so nobody ends up short.
		pending.sort((a, b) => needed.get(b)! - needed.get(a)!);
		const used = new Set<number>();
		const round: [number, number][] = [];
		for (const a of pending) {
			if (used.has(a)) {
				continue;
			}
			const partner =
				pending.find(
					(b) =>
						b !== a &&
						!used.has(b) &&
						cidByTid.get(b) !== cidByTid.get(a) &&
						!met.has(key(a, b)),
				) ??
				pending.find(
					(b) => b !== a && !used.has(b) && cidByTid.get(b) !== cidByTid.get(a),
				) ??
				// Last resort for the final stragglers: a conference foe.
				pending.find((b) => b !== a && !used.has(b));
			if (partner === undefined) {
				continue;
			}
			used.add(a);
			used.add(partner);
			met.add(key(a, partner));
			needed.set(a, needed.get(a)! - 1);
			needed.set(partner, needed.get(partner)! - 1);
			// Whoever has had fewer home games hosts.
			const aHome =
				homeGames.get(a)! === homeGames.get(partner)!
					? Math.random() < 0.5
					: homeGames.get(a)! < homeGames.get(partner)!;
			const game: [number, number] = aHome ? [a, partner] : [partner, a];
			homeGames.set(game[0], homeGames.get(game[0])! + 1);
			round.push(game);
		}
		if (round.length === 0) {
			break;
		}
		nonConfRounds.push(round);
	}

	return [...nonConfRounds, ...confRounds].flat();
};

export default newScheduleCollege;
