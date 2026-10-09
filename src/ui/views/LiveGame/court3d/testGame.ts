import { makeCourtRng } from "../courtRng.ts";
import { compileCourt, type CourtPlayer, type RawEvent } from "./director.ts";

// FOR THE TESTS: a small stand-in for GameSim.basketball's play-by-play,
// made up from a seed. The same event shapes, in the same orders the sim
// emits them (an attempt, then a make, a miss and a rebound, a block, or a
// shooting foul and its free throws; a putback with no attempt line;
// turnovers, steals, fouls, subs, timeouts and period breaks). Raw team 0 is
// home, as in the sim.
export const fakeGame = (seed: string, possessions: number) => {
	const rng = makeCourtRng(seed);
	const pick = <T>(arr: T[]): T => arr[Math.floor(rng() * arr.length)]!;
	const POS = ["PG", "SG", "SF", "PF", "C", "G", "F", "C"];
	const roster: [number[], number[]] = [
		[1, 2, 3, 4, 5, 6, 7, 8],
		[11, 12, 13, 14, 15, 16, 17, 18],
	];
	const players: CourtPlayer[] = [];
	for (const raw of [0, 1] as const) {
		roster[raw].forEach((pid, j) => {
			players.push({ pid, team: raw === 0 ? 1 : 0, pos: POS[j] });
		});
	}
	const onCourt: [number[], number[]] = [
		roster[0].slice(0, 5),
		roster[1].slice(0, 5),
	];
	const events: RawEvent[] = [{ type: "init", boxScore: {} }];
	for (const raw of [0, 1] as const) {
		for (const pid of onCourt[raw]) {
			events.push({ type: "stat", t: raw, pid, s: "gs", amt: 1 });
		}
	}
	let clock = 720;
	let period = 1;
	const tick = (lo: number, hi: number) => {
		clock = Math.max(0.5, clock - (lo + rng() * (hi - lo)));
		return Math.round(clock * 10) / 10;
	};
	events.push({
		type: "jumpBall",
		t: 0,
		pid: onCourt[0][4],
		pid2: onCourt[1][4],
		clock,
	});
	let o: 0 | 1 = 0;
	let putbackFor: number | undefined;

	const rebound = (shooterTeam: 0 | 1) => {
		const r = rng();
		if (r < 0.08) {
			// Out off the team that touched it last (as in the sim, mostly the
			// shooting team), and the other team's ball.
			const t = (rng() < 0.9 ? shooterTeam : 1 - shooterTeam) as 0 | 1;
			events.push({
				type: "outOfBounds",
				t,
				on: t === shooterTeam ? "offense" : "defense",
				clock: tick(0.2, 1),
			});
			o = (1 - t) as 0 | 1;
			return;
		}
		if (r < 0.75) {
			const d = (1 - shooterTeam) as 0 | 1;
			events.push({
				type: "drb",
				t: d,
				pid: pick(onCourt[d]),
				clock: tick(0.2, 1),
			});
			o = d;
		} else {
			const pid = pick(onCourt[shooterTeam]);
			events.push({ type: "orb", t: shooterTeam, pid, clock: tick(0.2, 1) });
			o = shooterTeam;
			if (rng() < 0.5) {
				putbackFor = pid;
			}
		}
	};
	const freeThrows = (team: 0 | 1, pid: number, n: number) => {
		for (let k = 0; k < n; k++) {
			const made = rng() < 0.75;
			events.push({ type: made ? "ft" : "missFt", t: team, pid, clock });
			if (made) {
				events.push({ type: "stat", t: team, pid, s: "pts", amt: 1 });
			}
			if (k === n - 1) {
				if (made) {
					o = (1 - team) as 0 | 1;
				} else {
					rebound(team);
				}
			}
		}
	};

	for (let n = 0; n < possessions; n++) {
		const d = (1 - o) as 0 | 1;
		if (n > 0 && n % 22 === 0) {
			events.push({ type: "endOfPeriod", t: o, reason: "noShot", clock: 0 });
			period += 1;
			clock = 720;
			events.push({ type: "period", period, clock });
			const off = onCourt[0].slice(0, 2);
			const on = roster[0].filter((p) => !onCourt[0].includes(p)).slice(0, 2);
			onCourt[0] = [...onCourt[0].filter((p) => !off.includes(p)), ...on];
			events.push({ type: "sub", t: 0, pids: on, pidsOff: off, clock });
		}
		if (rng() < 0.05) {
			events.push({
				type: "timeout",
				t: o,
				numLeft: 3,
				advancesBall: false,
				clock: tick(0, 0),
			});
		}
		if (rng() < 0.06) {
			const team = (rng() < 0.5 ? 0 : 1) as 0 | 1;
			const off = [pick(onCourt[team])];
			const on = [pick(roster[team].filter((p) => !onCourt[team].includes(p)))];
			onCourt[team] = [...onCourt[team].filter((p) => !off.includes(p)), ...on];
			events.push({ type: "sub", t: team, pids: on, pidsOff: off, clock });
		}

		if (putbackFor !== undefined && onCourt[o].includes(putbackFor)) {
			const pid = putbackFor;
			putbackFor = undefined;
			if (rng() < 0.55) {
				events.push(
					{
						type: "fgPutBack",
						t: o,
						pid,
						clock: tick(0.3, 1.2),
						period,
					},
					{ type: "stat", t: o, pid, s: "pts", amt: 2 },
				);
				o = d;
			} else {
				events.push({ type: "missPutBack", t: o, pid, clock: tick(0.3, 1.2) });
				rebound(o);
			}
			continue;
		}
		putbackFor = undefined;

		const r = rng();
		const shooter = pick(onCourt[o]);
		if (r < 0.7) {
			const zone = pick(["AtRim", "LowPost", "MidRange", "Tp"]);
			events.push({
				type: zone === "Tp" ? "fgaTp" : `fga${zone}`,
				t: o,
				pid: shooter,
				clock: tick(4, 18),
				desperation: false,
			});
			const res = rng();
			if (res < 0.45) {
				const ast =
					rng() < 0.6
						? pick(onCourt[o].filter((p) => p !== shooter))
						: undefined;
				const andOne = rng() < 0.05;
				const base = zone === "Tp" ? "tp" : `fg${zone}`;
				events.push(
					{
						type: andOne ? `${base}AndOne` : base,
						t: o,
						pid: shooter,
						pidAst: ast,
						pidDefense: zone === "AtRim" ? pick(onCourt[d]) : undefined,
						pidFoul: andOne ? pick(onCourt[d]) : undefined,
						clock: tick(0.2, 1.5),
						period,
					},
					{
						type: "stat",
						t: o,
						pid: shooter,
						s: "pts",
						amt: zone === "Tp" ? 3 : 2,
					},
				);
				if (andOne) {
					freeThrows(o, shooter, 1);
				} else {
					o = d;
				}
			} else if (res < 0.88) {
				events.push({
					type: zone === "Tp" ? "missTp" : `miss${zone}`,
					t: o,
					pid: shooter,
					clock: tick(0.2, 1.5),
				});
				rebound(o);
			} else if (res < 0.95) {
				events.push({
					type: zone === "Tp" ? "blkTp" : `blk${zone}`,
					t: d,
					pid: pick(onCourt[d]),
					clock: tick(0.1, 0.4),
				});
				rebound(o);
			} else {
				events.push({
					type: zone === "Tp" ? "pfTP" : "pfFG",
					t: d,
					pid: pick(onCourt[d]),
					pidShooting: shooter,
					clock,
				});
				freeThrows(o, shooter, zone === "Tp" ? 3 : 2);
			}
		} else if (r < 0.8) {
			events.push({
				type: "tov",
				t: o,
				pid: shooter,
				outOfBounds: rng() < 0.3,
				clock: tick(3, 12),
			});
			o = d;
		} else if (r < 0.88) {
			events.push({
				type: "stl",
				t: d,
				pid: pick(onCourt[d]),
				pidTov: shooter,
				outOfBounds: rng() < 0.1,
				clock: tick(3, 12),
			});
			o = d;
		} else if (r < 0.9) {
			// Knocked out of bounds by the defense: still their ball.
			events.push({
				type: "outOfBounds",
				t: d,
				on: "defense",
				clock: tick(0, 10),
			});
		} else if (r < 0.95) {
			events.push({
				type: "pfNonShooting",
				t: d,
				pid: pick(onCourt[d]),
				clock: tick(2, 10),
			});
		} else {
			events.push({
				type: "pfBonus",
				t: d,
				pid: pick(onCourt[d]),
				pidShooting: shooter,
				clock: tick(2, 10),
			});
			freeThrows(o, shooter, 2);
		}
	}
	events.push(
		{ type: "endOfPeriod", t: o, reason: "noShot", clock: 0 },
		{ type: "gameOver" },
	);
	return { events, players };
};

export const gidOf = (seed: string) =>
	[...seed].reduce((h, c) => (h * 31 + c.charCodeAt(0)) % 100_000, 7);

export const compile = (seed: string, possessions = 120) => {
	const { events, players } = fakeGame(seed, possessions);
	const tl = compileCourt({ events, players, gid: gidOf(seed) });
	return { events, players, tl };
};
