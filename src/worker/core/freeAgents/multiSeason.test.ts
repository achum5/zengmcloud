import { assert, beforeEach, describe, test, vi } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { player, team } from "../index.ts";
import autoSign from "./autoSign.ts";
import clearSpaceForSignings from "./clearSpace.ts";
import decreaseDemands from "./decreaseDemands.ts";
import newPhaseResignPlayers from "../phase/newPhaseResignPlayers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import {
	captureFrontOfficeLog,
	type FrontOfficeEntry,
} from "../../util/frontOfficeLog.ts";
import { getLeagueTradeContext } from "../trade/tradePosture.ts";

// ---------------------------------------------------------------------------
// CONSECUTIVE offseasons on ONE league.
//
// Every other test in this suite runs independent one-off offseasons, which
// cannot see drift: a payroll that ratchets up a little every year, one club
// quietly hoovering up every star, rosters that age and are never replenished,
// or a team that falls into a tier and can never climb out. Those only appear
// when the same league is run forward, with each year's outcome becoming the
// next year's starting position.
//
// This also drives the RE-SIGNING path (newPhaseResignPlayers), which the rest
// of the suite never touches - shouldLetWalk was only ever covered as a pure
// function, so nothing proved it survived contact with the phase around it.
// ---------------------------------------------------------------------------

const NUM_TEAMS = 10;
const FA_DAYS = 30;
const SEASONS = 8;

const makeRng = (seed: number) => {
	let s = seed >>> 0;
	return () => {
		s = (s * 1_664_525 + 1_013_904_223) >>> 0;
		return s / 4_294_967_296;
	};
};

const POSITIONS = ["PG", "SG", "SF", "PF", "C"];

const makePlayer = ({
	tid,
	ovr,
	pot,
	age,
	pos,
	amount,
	exp,
}: {
	tid: number;
	ovr: number;
	pot: number;
	age: number;
	pos: string;
	amount: number;
	exp: number;
}) => {
	const p: any = player.generate(
		tid,
		age,
		g.get("season") - age,
		true,
		DEFAULT_LEVEL,
	);
	const r = p.ratings.at(-1);
	r.ovr = ovr;
	r.pot = Math.max(ovr, pot);
	r.pos = pos;
	p.born.year = g.get("season") - age;
	p.contract = { amount, exp };
	p.injury = { type: "Healthy", gamesRemaining: 0 };
	p.value = ovr;
	p.valueNoPot = ovr;
	p.valueFuzz = ovr;
	p.valueNoPotFuzz = ovr;
	return p;
};

const stubLeagueDb = () => {
	const store: any = {
		get: async () => undefined,
		getAll: async () => [],
		put: async () => undefined,
		async *iterate() {},
		index: () => store,
	};
	idb.league = {
		get: async () => undefined,
		getAll: async () => [],
		transaction: () => ({
			store,
			objectStore: () => store,
			done: Promise.resolve(),
		}),
	} as any;
};

const build = async (rng: () => number) => {
	resetG();
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
	g.setWithoutSavingToDB("userTids", [-99]);
	g.setWithoutSavingToDB("salaryCapType", "soft");
	// Faces are irrelevant here and their generation draws from Math.random,
	// which these tests replace with a seeded sequence - leaving it on would
	// make every face feature shift the stream the economics run on.
	g.setWithoutSavingToDB("realisticFaces", false);

	const salaryCap = g.get("salaryCap");
	const minContract = g.get("minContract");
	const teams: any[] = [];
	const players: any[] = [];
	const draftPicks: any[] = [];

	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		teams.push(
			team.generate({
				tid,
				cid: 0,
				did: 0,
				region: `R${tid}`,
				name: `T${tid}`,
				abbrev: `T${tid}`,
				pop: 3,
				popRank: tid + 1,
				strategy: "contending",
			}),
		);
		const strength = rng();
		let budget = salaryCap * (0.55 + rng() * 0.35);
		for (let i = 0; i < 12; i++) {
			const share = i < 3 ? 0.2 : 0.045;
			const amount = Math.max(minContract, Math.round(budget * share));
			budget -= amount;
			players.push(
				makePlayer({
					tid,
					ovr: Math.round(
						42 + strength * 20 + (i < 3 ? 14 : 0) - i + rng() * 6,
					),
					pot: 0,
					age: Math.round(21 + rng() * 14),
					pos: POSITIONS[i % POSITIONS.length]!,
					amount,
					exp: g.get("season") + Math.floor(rng() * 4),
				}),
			);
		}
		for (const round of [1, 2]) {
			draftPicks.push({
				dpid: draftPicks.length,
				tid,
				originalTid: tid,
				round,
				pick: 0,
				season: g.get("season") + 1,
			});
		}
	}

	await resetCache({ players, teams, draftPicks });
	stubLeagueDb();
	return { salaryCap, minContract };
};

// A draft class for the coming year, so picks keep a value and there is fresh
// talent entering - without it the league can only get older and worse, and any
// decline this test measured would be the fixture's fault, not the AI's.
const addDraftClass = async (rng: () => number, season: number) => {
	const minContract = g.get("minContract");
	for (let i = 0; i < NUM_TEAMS * 2; i++) {
		const p: any = makePlayer({
			tid: PLAYER.UNDRAFTED,
			ovr: Math.round(44 - i / 3 + rng() * 6),
			pot: Math.round(62 - i / 4 + rng() * 10),
			age: 19,
			pos: POSITIONS[i % POSITIONS.length]!,
			amount: minContract,
			exp: season + 3,
		});
		p.draft.year = season;
		await idb.cache.players.add(p);
	}
};

// Records that follow roster strength, so postures mean something each year.
const setRecords = async () => {
	const context = await getLeagueTradeContext();
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const rank = context.teamOvrsSorted.findIndex((x) => x.tid === tid);
		const winp = 0.8 - (0.6 * rank) / Math.max(1, NUM_TEAMS - 1);
		const existing = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsBySeasonTid",
			[g.get("season"), tid],
		);
		const row: any =
			existing ?? team.genSeasonRow((await idb.cache.teams.get(tid))!);
		row.season = g.get("season");
		row.tid = tid;
		row.won = Math.round(82 * winp);
		row.lost = 82 - row.won;
		row.gp = 82;
		if (existing) {
			await idb.cache.teamSeasons.put(row);
		} else {
			await idb.cache.teamSeasons.add(row);
		}
	}
};

type Year = {
	season: number;
	avgPayroll: number;
	maxPayroll: number;
	overCap: number;
	shortRosters: number;
	avgRosterSize: number;
	avgAge: number;
	topHeavy: number;
	// topHeavy only measures CONCENTRATION, so on its own it cannot tell "talent
	// is spread more evenly" apart from "the league got worse". These two measure
	// the level: rotation quality per team, and total stars employed league-wide.
	leagueOvr: number;
	starsEmployed: number;
	// Talent ON rosters vs talent LEFT in the pool. If the smart league is
	// rostering worse players while better ones sit unsigned, that is a signing
	// defect; if both pools look the same, the difference is the fixture.
	rosterOver50: number;
	faOver50: number;
	freeAgents: number;
	dumps: number;
	// Measured at the seam between re-signing and free agency, so a payroll gap
	// against vanilla can be attributed to one half or the other instead of
	// being a single number nobody can explain.
	payrollAfterResign: number;
	walkedToFa: number;
	overpays: number;
};

describe("eight consecutive offseasons on one league", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	const runLeague = async (
		smart: boolean,
		seed: number,
		// Flip smartAiFrontOffice ON at the start of this year (0-based), the way
		// a real league flips the setting between seasons. The years before it run
		// vanilla - byte-identical to a runLeague(false, ...) of the same seed,
		// which is what lets the flip test attribute every post-flip difference to
		// the flip itself.
		enableAtYear?: number,
	) => {
		const rng = makeRng(seed);

		// The spy covers EVERYTHING, fixture construction included. player.generate
		// reaches for BBGM's global RNG, which bottoms out in Math.random, so any
		// setup left outside the spy hands the two arms different leagues - and
		// then every difference between them is partly just noise. Star counts
		// swung by 7 between supposedly identical runs before this, which is wider
		// than the effect being measured.
		const spy = vi.spyOn(Math, "random").mockImplementation(rng);
		try {
			const { salaryCap } = await build(rng);
			g.setWithoutSavingToDB("smartAiFrontOffice", smart);

			const years: Year[] = [];
			const allEntries: FrontOfficeEntry[] = [];
			for (let year = 0; year < SEASONS; year++) {
				if (enableAtYear !== undefined && year === enableAtYear) {
					g.setWithoutSavingToDB("smartAiFrontOffice", true);
				}
				const season = g.get("season");
				await addDraftClass(rng, season + 1);
				await setRecords();

				const faBefore = (
					await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
				).length;
				let payrollAfterResign = 0;
				let walkedToFa = 0;

				const capture = captureFrontOfficeLog();
				{
					// Re-signing, then free agency - the real offseason order.
					g.setWithoutSavingToDB("phase", PHASE.RESIGN_PLAYERS);
					await newPhaseResignPlayers({} as any);

					const resignPayrolls: number[] = [];
					for (let tid = 0; tid < NUM_TEAMS; tid++) {
						resignPayrolls.push(await team.getPayroll(tid));
					}
					payrollAfterResign = Math.round(
						resignPayrolls.reduce((a, x) => a + x, 0) / NUM_TEAMS,
					);
					walkedToFa =
						(
							await idb.cache.players.indexGetAll(
								"playersByTid",
								PLAYER.FREE_AGENT,
							)
						).length - faBefore;

					g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
					for (let day = FA_DAYS; day > 0; day--) {
						g.setWithoutSavingToDB("daysLeft", day);
						await decreaseDemands();
						await clearSpaceForSignings();
						await autoSign();
					}
					// The real game tops rosters back up when games are about to start
					// (game/play.ts), so a thin roster during the offseason is normal and
					// measuring before this point would just be measuring the harness.
					await team.checkRosterSizes("other");
				}
				const entries = capture.stop();
				allEntries.push(...entries.map((e) => ({ ...e, season })));

				// Measure.
				const payrolls: number[] = [];
				const sizes: number[] = [];
				const ages: number[] = [];
				let topHeavy = 0;
				let starsEmployed = 0;
				let rosterOver50 = 0;
				const topOvrs: number[] = [];
				for (let tid = 0; tid < NUM_TEAMS; tid++) {
					payrolls.push(await team.getPayroll(tid));
					const roster = await idb.cache.players.indexGetAll(
						"playersByTid",
						tid,
					);
					sizes.push(roster.length);
					for (const p of roster) {
						ages.push(season - p.born.year);
					}
					const stars = roster.filter(
						(p) => p.ratings.at(-1)!.ovr >= 60,
					).length;
					starsEmployed += stars;
					rosterOver50 += roster.filter(
						(p) => p.ratings.at(-1)!.ovr >= 50,
					).length;
					if (stars >= 5) {
						topHeavy += 1;
					}
					// team.ovr, not a mean of the best raw OVRs. A position-blind mean
					// is biased against the entire point of fit-based signing: a team
					// that takes the 52 centre it needs over the 55 guard it does not
					// scores WORSE on raw OVR while fielding a better side. team.ovr
					// is how the game itself judges a roster, so it is the honest
					// measure of whether these decisions produce better teams.
					topOvrs.push(
						team.ovr(
							roster.map((p) => ({
								pid: p.pid,
								injury: p.injury,
								value: p.value,
								ratings: {
									ovr: p.ratings.at(-1)!.ovr,
									ovrs: p.ratings.at(-1)!.ovrs,
									pos: p.ratings.at(-1)!.pos,
								},
							})),
						),
					);
				}
				const mean = (xs: number[]) =>
					xs.length ? xs.reduce((a, x) => a + x, 0) / xs.length : 0;

				years.push({
					season,
					avgPayroll: Math.round(mean(payrolls)),
					maxPayroll: Math.round(Math.max(...payrolls)),
					overCap: payrolls.filter((p) => p > salaryCap * 1.4).length,
					shortRosters: sizes.filter((s) => s < g.get("minRosterSize")).length,
					avgRosterSize: Math.round(mean(sizes) * 10) / 10,
					avgAge: Math.round(mean(ages) * 10) / 10,
					topHeavy,
					leagueOvr: Math.round(mean(topOvrs) * 10) / 10,
					starsEmployed,
					rosterOver50,
					faOver50: (
						await idb.cache.players.indexGetAll(
							"playersByTid",
							PLAYER.FREE_AGENT,
						)
					).filter((p) => p.ratings.at(-1)!.ovr >= 50).length,
					freeAgents: (
						await idb.cache.players.indexGetAll(
							"playersByTid",
							PLAYER.FREE_AGENT,
						)
					).length,
					dumps: entries.filter((e) => e.event === "dump-and-sign").length,
					payrollAfterResign,
					walkedToFa,
					overpays: entries.filter((e) => e.event === "retention-overpay")
						.length,
				});

				// Roll the league forward a year: everyone ages, contracts tick down.
				g.setWithoutSavingToDB("season", season + 1);
				for (const p of await idb.cache.players.indexGetAll("playersByTid", [
					0,
					Infinity,
				])) {
					await idb.cache.players.put(p);
				}
			}

			// Who the market refused to employ. If the league is leaving good
			// players unsigned, this is where it shows up with enough detail to
			// say WHY.
			const season = g.get("season") - 1;
			const finalPayrolls: number[] = [];
			for (let tid = 0; tid < NUM_TEAMS; tid++) {
				finalPayrolls.push(await team.getPayroll(tid));
			}
			const unsigned = (
				await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
			)
				.filter((p) => p.ratings.at(-1)!.ovr >= 60)
				.map((p) => {
					// "couldFit" separates the two possible explanations: if no team
					// could have afforded him, this is a cap-room story and the
					// ordering is innocent. If several could, ordering passed him over.
					const couldFit = finalPayrolls.filter(
						(pay) => pay + p.contract.amount <= salaryCap,
					).length;
					const r = p.ratings.at(-1)!;
					return {
						ovr: r.ovr,
						age: season - p.born.year,
						label: `ovr ${r.ovr} age ${season - p.born.year} $${Math.round(p.contract.amount)} thru ${p.contract.exp} ${r.pos} couldFit=${couldFit}`,
					};
				});

			// team.ovr weights a team's BEST player most heavily, so the league mean
			// is maximised by spreading talent evenly and depressed by hoarding it.
			// A low league mean therefore means concentration, not scarcity - and
			// only the per-team spread can tell those apart.
			const finalOvrs: number[] = [];
			const finalSizes: number[] = [];
			for (let tid = 0; tid < NUM_TEAMS; tid++) {
				const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
				finalSizes.push(roster.length);
				finalOvrs.push(
					team.ovr(
						roster.map((p) => ({
							pid: p.pid,
							injury: p.injury,
							value: p.value,
							ratings: {
								ovr: p.ratings.at(-1)!.ovr,
								ovrs: p.ratings.at(-1)!.ovrs,
								pos: p.ratings.at(-1)!.pos,
							},
						})),
					),
				);
			}

			return {
				years,
				allEntries,
				salaryCap,
				unsigned,
				finalOvrs: finalOvrs.map((x) => Math.round(x)).sort((a, b) => a - b),
				finalSizes: finalSizes.slice().sort((a, b) => a - b),
			};
		} finally {
			spy.mockRestore();
		}
	};

	const table = (label: string, years: Year[]) =>
		[
			"",
			`=== ${label} ===`,
			"season  postResign   avgPay   maxPay  roster  age   teamOvr  stars  ros50  fa50  FAs  walked  dumps  keep",
			...years.map(
				(y) =>
					`${y.season}   ${String(y.payrollAfterResign).padStart(9)} ${String(y.avgPayroll).padStart(8)} ${String(y.maxPayroll).padStart(8)}  ${String(y.avgRosterSize).padStart(5)}  ${String(y.avgAge).padStart(4)}  ${String(y.leagueOvr).padStart(6)}  ${String(y.starsEmployed).padStart(5)}  ${String(y.rosterOver50).padStart(5)} ${String(y.faOver50).padStart(5)}  ${String(y.freeAgents).padStart(4)}  ${String(y.walkedToFa).padStart(5)}  ${String(y.dumps).padStart(4)}  ${String(y.overpays).padStart(4)}`,
			),
		].join("\n");

	// The question that actually matters: is a league run by the smart front
	// offices in WORSE shape than the same league run by stock BBGM? Absolute
	// thresholds are guesses; vanilla is the only honest baseline.
	//
	// Several seeds, because they genuinely disagree - one seed put a 32-year-old
	// 72 ovr out of work while another cleared the market completely, and a single
	// seed would have called whichever it drew "the" behaviour.
	// Talent-employed shortfalls, one per seed, judged together once every seed
	// has run. See the note at the collection site for why this is not a per-seed
	// assertion.
	const shortfalls: number[] = [];

	for (const seed of [31, 1234, 5150]) {
		test(`eight seasons of smart AI leaves the league no worse than vanilla (seed ${seed})`, async () => {
			const smart = await runLeague(true, seed);
			const vanilla = await runLeague(false, seed);

			console.log(
				[
					table("SMART AI", smart.years),
					table("VANILLA", vanilla.years),
					"",
					`smart dump-and-sign deals: ${smart.allEntries.filter((e) => e.event === "dump-and-sign").length}`,
					`gave-up detail: ${smart.allEntries
						.filter((e) => e.event === "retention-gave-up")
						.slice(0, 12)
						.map(
							(e) =>
								`ovr${e.data.ovr} p=${(e.data.probWilling as number).toFixed(3)} max=${e.data.maxMultiplier} tries=${e.data.attempts} ask=${e.data.asked}/${e.data.maxContract}`,
						)
						.join(" | ")}`,
					`retention outcomes: ${JSON.stringify(
						Object.fromEntries(
							[
								"retention-overpay",
								"retention-gave-up",
								"retention-not-worth-it",
							].map((k) => [
								k,
								smart.allEntries.filter((e) => e.event === k).length,
							]),
						),
					)}`,
					`vanilla front-office decisions: ${vanilla.allEntries.length} (must be 0)`,
					"",
					`unsigned stars, SMART (${smart.unsigned.length}):`,
					...smart.unsigned.map((x) => `  ${x.label}`),
					`unsigned stars, VANILLA (${vanilla.unsigned.length}):`,
					...vanilla.unsigned.map((x) => `  ${x.label}`),
					"",
					`final team ovrs SMART:   ${smart.finalOvrs.join(" ")}`,
					`final team ovrs VANILLA: ${vanilla.finalOvrs.join(" ")}`,
					`final roster sizes SMART:   ${smart.finalSizes.join(" ")}`,
					`final roster sizes VANILLA: ${vanilla.finalSizes.join(" ")}`,
					`worst useful-player shortfall vs vanilla: ${Math.max(
						...smart.years.map(
							(y, i) => vanilla.years[i]!.rosterOver50 - y.rosterOver50,
						),
					)}`,
					"",
				].join("\n"),
			);

			// The switch has to be a real switch.
			assert.strictEqual(
				vanilla.allEntries.length,
				0,
				"vanilla run still made front-office decisions",
			);

			const lastSmart = smart.years.at(-1)!;
			const lastVanilla = vanilla.years.at(-1)!;

			// Nobody unable to field a team once the game would have topped rosters up.
			for (const y of smart.years) {
				assert.strictEqual(
					y.shortRosters,
					0,
					`season ${y.season}: ${y.shortRosters} teams below the roster minimum`,
				);
			}

			// Payroll must not run away relative to vanilla.
			assert.ok(
				lastSmart.avgPayroll < lastVanilla.avgPayroll * 1.5,
				`smart payroll ran away: ${lastSmart.avgPayroll} vs vanilla ${lastVanilla.avgPayroll}`,
			);

			// Talent must not pool in a few clubs any worse than it already does.
			const worstSmart = Math.max(...smart.years.map((y) => y.topHeavy));
			const worstVanilla = Math.max(...vanilla.years.map((y) => y.topHeavy));
			assert.ok(
				worstSmart <= worstVanilla + 2,
				`talent pooling worse than vanilla: ${worstSmart} vs ${worstVanilla}`,
			);

			// The market must clear about as well as it does in vanilla - a smart AI
			// that hoards cap space and leaves players unsigned would show up here.
			assert.ok(
				lastSmart.freeAgents <= lastVanilla.freeAgents * 1.5 + 20,
				`free agent pool silting up: ${lastSmart.freeAgents} vs vanilla ${lastVanilla.freeAgents}`,
			);

			// Rosters must not be systematically thinner.
			assert.ok(
				lastSmart.avgRosterSize >= lastVanilla.avgRosterSize - 1.5,
				`rosters thinner than vanilla: ${lastSmart.avgRosterSize} vs ${lastVanilla.avgRosterSize}`,
			);

			// The league must keep employing its best players. This is the assertion
			// that caught the worst regression this feature ever had: fit adjustments
			// multiply, and because age and contract risk point the same way at nearly
			// every team, a good-but-old player was buried at ALL of them at once and
			// simply never signed. Stars employed decayed 36 -> 29 over eight seasons
			// while vanilla employed all 36 every year. Nothing else in this file
			// noticed - payroll, roster size and the free agent COUNT all looked fine,
			// because a star going unsigned and a scrub going unsigned look identical
			// unless you weigh them.
			for (const [i, y] of smart.years.entries()) {
				const v = vanilla.years[i]!;
				assert.ok(
					y.starsEmployed >= v.starsEmployed - 4,
					`season ${y.season}: smart employs ${y.starsEmployed} stars vs vanilla ${v.starsEmployed}`,
				);
			}

			// No team may be stripped to the point of not fielding a side. A rebuild
			// is allowed to be bad; it is not allowed to be an absorbing state that
			// leaves a club on the bare minimum forever. This runs BEFORE the talent
			// check below because it is the sharper instrument for that failure -
			// it caught teardown-as-absorbing-state at 10-man rosters.
			assert.ok(
				Math.min(...smart.finalSizes) >= g.get("minRosterSize") + 2,
				`a team was stripped bare: roster sizes ${smart.finalSizes.join(" ")}`,
			);

			// Talent EMPLOYED, which is distribution-neutral on purpose.
			//
			// The obvious assertion here - that the smart league's mean team ovr keeps
			// up with vanilla's - is the wrong one, and worth spelling out because it
			// looks so reasonable. team.ovr weights each team's best players most
			// heavily, so the league mean is maximised by spreading talent perfectly
			// evenly and depressed by ANY concentration. Contenders loading up while
			// rebuilders go young is the entire feature; that assertion would have
			// demanded the feature do nothing. Worse, the vanilla arm cannot stratify
			// even in principle here - team.updateStrategies needs per-player minutes
			// and this fixture plays no games, so every vanilla team stays frozen on
			// the strategy it was built with.
			//
			// Talent employed is recorded per seed and judged in aggregate at the
			// bottom of the file, NOT asserted here.
			//
			// Asserting it per seed against a threshold was a methodology mistake
			// worth leaving a note about, because the numbers looked convincing. One
			// seed drifted past the line and the obvious reading was that the
			// retention overpay had cost the league ten jobs - except that seed had
			// recorded exactly ONE overpay, and one signing cannot cost ten jobs.
			// Measured across ten seeds the mean shortfall was 5.3 with the feature
			// and 5.4 without it, and seeds with ZERO overpays produced shortfalls of
			// 9. The metric is chaotic over an eight-season horizon: any change to
			// any decision reshuffles the random stream and compounds. Only the
			// aggregate says anything.
			shortfalls.push(
				Math.max(
					...smart.years.map(
						(y, i) => vanilla.years[i]!.rosterOver50 - y.rosterOver50,
					),
				),
			);

			// A player young enough to still be good, left unemployed, is never a
			// defensible front-office outcome. (Old unsigned players are largely a
			// fixture artifact: this harness never retires anybody, so it accumulates
			// 40-year-olds who would not exist in a real league.)
			const primeUnsigned = smart.unsigned.filter(
				(x) => x.age <= 33 && x.ovr >= 65,
			);
			assert.ok(
				primeUnsigned.length <= 1,
				`prime-age stars left unemployed: ${primeUnsigned.map((x) => x.label).join("; ")}`,
			);
		}, 600_000);
	}

	test("across seeds, smart AI wastes no more talent than vanilla", () => {
		assert.strictEqual(
			shortfalls.length,
			3,
			"a seed did not report, so this average is not over what it claims",
		);
		const mean = shortfalls.reduce((a, x) => a + x, 0) / shortfalls.length;
		console.log(
			`\nshortfalls by seed: ${shortfalls.join(", ")} (mean ${mean.toFixed(1)})\n`,
		);

		// Benchmarked over ten seeds with the retention overpay on and off: mean
		// 5.3 against 5.4, individual seeds ranging 1-11 in BOTH arms. So the bar
		// sits on the mean, where the noise cancels, and at 9 - comfortably above
		// both measured means, and comfortably below a real regression, which would
		// move every seed rather than one.
		assert.ok(
			mean <= 9,
			`smart AI leaves ${mean.toFixed(1)} more useful players unemployed per seed than vanilla`,
		);
	});

	// -------------------------------------------------------------------------
	// FLIPPING IT ON MID-LEAGUE.
	//
	// Every run above enables the feature at year zero, on a fresh fixture. A
	// real league does neither: it flips the setting years in, on rosters and
	// payrolls the VANILLA rules accumulated - old cores a smart AI would never
	// have assembled, contracts it would never have signed. The postures computed
	// from that inherited state are the feature's first-ever look at the league,
	// and the fear is a transition-year shock: half the league's veterans walking
	// at once because shouldLetWalk suddenly applies to everybody, or a wave of
	// salary dumps as every capped-out club discovers "relief" the same morning.
	//
	// The control is the same seed run vanilla throughout, so the pre-flip years
	// are byte-identical (asserted - it proves every post-flip difference is the
	// flip) and each post-flip year has an exact counterfactual to answer to.
	// -------------------------------------------------------------------------
	describe("flipping smart AI on in an established league", () => {
		const FLIP_YEAR = 4;

		for (const seed of [31, 1234]) {
			test(`no transition shock, and no worse than vanilla after (seed ${seed})`, async () => {
				const flipped = await runLeague(false, seed, FLIP_YEAR);
				const vanilla = await runLeague(false, seed);

				console.log(
					[
						table(`FLIPPED ON AT YEAR ${FLIP_YEAR}`, flipped.years),
						table("VANILLA CONTROL", vanilla.years),
						"",
					].join("\n"),
				);

				// The pre-flip years must be IDENTICAL - same seed, same rules, same
				// decisions. If they are not, the harness is comparing two different
				// leagues and nothing after this line means anything.
				for (let i = 0; i < FLIP_YEAR; i++) {
					assert.deepStrictEqual(
						flipped.years[i],
						vanilla.years[i],
						`pre-flip year ${i} diverged from the control`,
					);
				}

				const transition = flipped.years[FLIP_YEAR]!;
				const control = vanilla.years[FLIP_YEAR]!;

				// The transition-year shock checks. walkedToFa is the direct measure
				// of the feared failure: shouldLetWalk waking up on four seasons of
				// vanilla-accumulated veterans. The bar allows one extra walk-away per
				// team over the control before it calls shock - far above a strategic
				// trickle, far below an exodus.
				assert.ok(
					transition.walkedToFa <= control.walkedToFa + NUM_TEAMS,
					`transition-year exodus: ${transition.walkedToFa} walked to FA vs ${control.walkedToFa} in the control`,
				);

				// One dump per team per offseason is the hard cap; the fear is the
				// whole league using it at once the year the feature wakes up.
				assert.ok(
					transition.dumps <= Math.ceil(NUM_TEAMS / 2),
					`dump wave in the transition year: ${transition.dumps} dumps`,
				);

				// From the flip on, the same league-health bars as the main test: the
				// stars stay employed, nobody fields an illegal side, payroll doesn't
				// run away.
				for (let i = FLIP_YEAR; i < SEASONS; i++) {
					const y = flipped.years[i]!;
					const v = vanilla.years[i]!;
					assert.strictEqual(
						y.shortRosters,
						0,
						`season ${y.season}: ${y.shortRosters} teams below the roster minimum`,
					);
					assert.ok(
						y.starsEmployed >= v.starsEmployed - 4,
						`season ${y.season}: flipped league employs ${y.starsEmployed} stars vs vanilla ${v.starsEmployed}`,
					);
				}

				const lastFlipped = flipped.years.at(-1)!;
				const lastVanilla = vanilla.years.at(-1)!;
				assert.ok(
					lastFlipped.avgPayroll < lastVanilla.avgPayroll * 1.5,
					`payroll ran away after the flip: ${lastFlipped.avgPayroll} vs vanilla ${lastVanilla.avgPayroll}`,
				);
				assert.ok(
					lastFlipped.freeAgents <= lastVanilla.freeAgents * 1.5 + 20,
					`free agent pool silting up after the flip: ${lastFlipped.freeAgents} vs vanilla ${lastVanilla.freeAgents}`,
				);
				assert.ok(
					Math.min(...flipped.finalSizes) >= g.get("minRosterSize") + 2,
					`a team was stripped bare after the flip: roster sizes ${flipped.finalSizes.join(" ")}`,
				);

				// Prime-age stars unemployed - same bar as the main test. The flip
				// must not strand players the vanilla years signed happily.
				const primeUnsigned = flipped.unsigned.filter(
					(x) => x.age <= 33 && x.ovr >= 65,
				);
				assert.ok(
					primeUnsigned.length <= 1,
					`prime-age stars left unemployed after the flip: ${primeUnsigned.map((x) => x.label).join("; ")}`,
				);
			}, 600_000);
		}
	});
});
