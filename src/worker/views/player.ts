import {
	PLAYER,
	PLAYER_STATS_TABLES,
	RATINGS,
	PLAYER_SUMMARY,
	DEFAULT_JERSEY,
} from "../../common/constants.ts";
import { player } from "../core/index.ts";
import { idb } from "../db/index.ts";
import {
	coarsenRating,
	exemptFromCoarseRatings,
} from "../../common/coarsenRating.ts";
import { g, helpers } from "../util/index.ts";
import type {
	MenuItemHeader,
	MenuItemLink,
	MinimalPlayerRatings,
	Player,
	UpdateEvents,
	ViewInput,
} from "../../common/types.ts";
import { orderBy } from "../../common/utils.ts";
import { isSport } from "../../common/sportFunctions.ts";
import { formatEventText } from "../util/formatEventText.ts";
import { upgradeFace } from "../util/face.ts";
import { choice } from "../../common/random.ts";
import { getTeamColors } from "../util/getTeamColors.ts";
import { getTeamInfoBySeason } from "../util/getTeamInfoBySeason.ts";
import { processPlayersHallOfFame } from "../util/processPlayersHallOfFame.ts";
import { getNoteTeammates } from "../util/getNoteTeammates.ts";
import {
	loadContractValueContexts,
	valueForPlayer,
} from "../util/contractValues.ts";
import type { ContractValueBreakdown } from "../../common/contractValue.ts";

export const getPlayerProfileStats = () => {
	const stats = [];
	for (const info of Object.values(PLAYER_STATS_TABLES)) {
		stats.push(...info.stats);
	}

	return Array.from(new Set(stats));
};

export const getPlayer = async (
	pRaw: Player,
	seasonRange?: [number, number],
	// Restrict the aggregated careerStats to a single team (for per-team career
	// totals). Filters the stat rows exactly like the rest of playersPlus does.
	tid?: number,
	// Restrict to an arbitrary set of seasons (for a selected-rows subtotal).
	seasons?: number[],
) => {
	type Stats = {
		season: number;
		tid: number;
		abbrev: string;
		age: number;
		playoffs: boolean;
		jerseyNumber: string;
	} & Record<string, number>;

	const stats = getPlayerProfileStats();

	const p:
		| (Pick<
				Player,
				| "pid"
				| "tid"
				| "hgt"
				| "weight"
				| "born"
				| "contract"
				| "diedYear"
				| "face"
				| "appearances"
				| "imgURL"
				| "injury"
				| "injuries"
				| "college"
				| "relatives"
				| "awards"
				| "srID"
		  > & {
				age: number;
				ageAtDeath: number | null;
				draft: Player["draft"] & {
					age: number;
					abbrev: string;
					originalAbbrev: string;
				};
				name: string;
				abbrev: string;
				mood: any;
				salaries: {
					amount: number;
					season: number;
					type: "past" | "current" | "future";
				}[];
				salariesTotal: any;
				untradable: any;
				untradableMsg?: string;
				ratings: (MinimalPlayerRatings & {
					abbrev: string;
					age: number;
					tid: number;
				})[];
				stats: Stats[];
				careerStats: Stats;
				careerStatsCombined: Stats;
				careerStatsPlayoffs: Stats;
				jerseyNumber?: string;
				experience: number;
				note?: string;
				watch: number;
		  })
		| undefined = await idb.getCopy.playersPlus(pRaw, {
		attrs: [
			"pid",
			"name",
			"tid",
			"abbrev",
			"age",
			"ageAtDeath",
			"hgt",
			"weight",
			"born",
			"diedYear",
			"contract",
			"draft",
			"face",
			"appearances",
			"mood",
			"injury",
			"injuries",
			"salaries",
			"salariesTotal",
			"awards",
			"imgURL",
			"watch",
			"college",
			"relatives",
			"untradable",
			"jerseyNumber",
			"experience",
			"note",
			"srID",
		],
		ratings: [
			"season",
			"abbrev",
			"tid",
			"age",
			"ovr",
			"pot",
			...RATINGS,
			"skills",
			"pos",
			"injuryIndex",
		],
		stats: ["season", "tid", "abbrev", "age", "jerseyNumber", ...stats],
		playoffs: true,
		combined: true,
		showRookies: true,
		fuzz: true,
		// The player page is where a career is read season by season, so a draft
		// class exempted from coarse ratings stays exempt here even after he's
		// drafted - opening his prospect year still shows the scouting report you
		// were given at the time.
		prospectSeasonsExact: true,
		mergeStats: "totAndTeams",
		seasonRange,
		seasons,
		tid,
	});

	if (!p) {
		return;
	}

	// Filter out rows with no games played
	p.stats = p.stats.filter((row) => row.gp! > 0);

	return p;
};

export const getCommon = async (
	pid: number | undefined,
	season: number | undefined,
	view: "player" | "player_game_log",
) => {
	if (pid === undefined) {
		// https://stackoverflow.com/a/59923262/786644
		const returnValue = {
			type: "error" as const,
			errorMessage: "Player not found.",
		};
		return returnValue;
	}

	const pRaw = await idb.getCopy.players(
		{
			pid,
		},
		"noCopyCache",
	);

	if (!pRaw) {
		// https://stackoverflow.com/a/59923262/786644
		const returnValue = {
			type: "error" as const,
			errorMessage: "Player not found.",
		};
		return returnValue;
	}

	await upgradeFace(pRaw);

	const p = await getPlayer(pRaw);

	if (!p) {
		// https://stackoverflow.com/a/59923262/786644
		const returnValue = {
			type: "error" as const,
			errorMessage: "Player not found.",
		};
		return returnValue;
	}

	if (p.tid !== PLAYER.RETIRED) {
		p.mood = await player.moodInfos(pRaw);

		// Account for extra free agent demands
		if (p.tid === PLAYER.FREE_AGENT) {
			p.contract.amount = p.mood.user.contractAmount / 1000;
		}
	}

	const willingToSign = !!(p.mood && p.mood.user && p.mood.user.willing);

	const retired = p.tid === PLAYER.RETIRED;

	let teamName = "";
	if (p.tid >= 0) {
		teamName = `${g.get("teamInfoCache")[p.tid]?.region} ${
			g.get("teamInfoCache")[p.tid]?.name
		}`;
	} else if (p.tid === PLAYER.FREE_AGENT) {
		teamName = "Free Agent";
	} else if (
		p.tid === PLAYER.UNDRAFTED ||
		p.tid === PLAYER.UNDRAFTED_FANTASY_TEMP
	) {
		teamName = "Draft Prospect";
	} else if (p.tid === PLAYER.RETIRED) {
		teamName = "Retired";
	}

	const teams = await idb.cache.teams.getAll();

	const jerseyNumberInfos: {
		number: string;
		start: number;
		end: number;
		t?: {
			tid: number;
			colors: [string, string, string];
			jersey?: string;
			name: string;
			region: string;
		};
		retiredIndex: number;
	}[] = [];
	let prevKey: string = "";
	for (const ps of p.stats) {
		const jerseyNumber = ps.jerseyNumber;
		if (jerseyNumber === undefined || ps.gp === 0 || ps.tid === PLAYER.TOT) {
			continue;
		}

		const ts = await getTeamInfoBySeason(ps.tid, ps.season);
		let t;
		if (ts && ts.colors && ts.name !== undefined && ts.region !== undefined) {
			t = {
				tid: ps.tid,
				colors: ts.colors,
				jersey: ts.jersey,
				name: ts.name,
				region: ts.region,
			};
		}

		// Don't include jersey in key, because it's not visible in the jersey number display
		const key = JSON.stringify([
			jerseyNumber,
			t?.tid,
			t?.colors?.map((x) => x.toUpperCase()),
			t?.name,
			t?.region,
		]);

		if (key === prevKey) {
			const prev = jerseyNumberInfos.at(-1)!;
			prev.end = ps.season;
		} else {
			const t2 = teams[ps.tid];
			const retiredIndex =
				t2?.retiredJerseyNumbers?.findIndex(
					(info) => info.pid === pid && info.number === jerseyNumber,
				) ?? -1;

			jerseyNumberInfos.push({
				number: jerseyNumber,
				start: ps.season,
				end: ps.season,
				t,
				retiredIndex,
			});
		}

		prevKey = key;
	}

	// WHAT UNIFORM HE WORE THAT YEAR.
	//
	// The appearance gallery stacks a headshot for every season of a career, and
	// dressing all of them in today's jersey makes a four-team journeyman look
	// like a one-team lifer. The colors come from the team AS IT WAS, not as it
	// is: a franchise that has since rebranded or relocated is the wrong picture
	// of 2011, and teamSeasons remembers what it actually looked like.
	//
	// One uniform per season, picked by games played, so a midseason trade shows
	// the jersey he spent the year in rather than whichever stats row happened
	// to sort last. Playoff rows are skipped because they duplicate the team
	// with a smaller sample.
	const appearanceTeams: Record<
		number,
		{
			abbrev: string;
			colors: [string, string, string];
			imgURL?: string;
			imgURLSmall?: string;
			jersey?: string;
			name: string;
			region: string;
			jerseyNumber?: string;
		}
	> = {};
	{
		const gpBySeason: Record<number, number> = {};
		for (const ps of p.stats) {
			// The typeof checks matter: playersPlus rows can carry tid as
			// undefined, and undefined < 0 is false - an undefined smuggled into
			// getTeamInfoBySeason becomes an invalid IndexedDB key, and that
			// DataError killed the entire player page.
			if (
				ps.playoffs ||
				typeof ps.tid !== "number" ||
				ps.tid < 0 ||
				typeof ps.season !== "number"
			) {
				continue;
			}
			const gp = ps.gp ?? 0;
			// Ties go to the later row, which for a deadline trade is the team
			// he finished the season with.
			if (gpBySeason[ps.season] !== undefined && gp < gpBySeason[ps.season]!) {
				continue;
			}
			const ts = await getTeamInfoBySeason(ps.tid, ps.season);
			if (!ts) {
				continue;
			}
			gpBySeason[ps.season] = gp;
			appearanceTeams[ps.season] = {
				abbrev: ts.abbrev,
				colors: ts.colors,
				imgURL: ts.imgURL,
				imgURLSmall: ts.imgURLSmall,
				jersey: ts.jersey,
				name: ts.name,
				region: ts.region,
				jerseyNumber: ps.jerseyNumber,
			};
		}

		// A stats row only exists once the regular season starts, so between the
		// draft and opening night a player has a ratings row for the new season
		// and nothing else - and the gallery would show his newest season as
		// "No team". The ratings rows carry a tid too, so they fill the gap.
		//
		// A ratings row's tid is DERIVED - playersPlus works it out from that
		// season's stats rows, so for exactly the seasons this loop exists for
		// (no stats yet) it is often undefined, and for a draft prospect it is
		// undefined or negative. Both are genuinely "no team" and stay empty;
		// only a real tid is a key worth looking up.
		for (const pr of p.ratings) {
			if (
				appearanceTeams[pr.season] !== undefined ||
				typeof pr.tid !== "number" ||
				pr.tid < 0
			) {
				continue;
			}
			const ts = await getTeamInfoBySeason(pr.tid, pr.season);
			if (!ts) {
				continue;
			}
			appearanceTeams[pr.season] = {
				abbrev: ts.abbrev,
				colors: ts.colors,
				imgURL: ts.imgURL,
				imgURLSmall: ts.imgURLSmall,
				jersey: ts.jersey,
				name: ts.name,
				region: ts.region,
				jerseyNumber:
					pr.season === g.get("season") ? p.jerseyNumber : undefined,
			};
		}
	}

	let teamColors;
	let teamJersey;
	let bestPos;
	if (p.tid === PLAYER.RETIRED) {
		const info = processPlayersHallOfFame([p])[0]!;
		const legacyTid = info.legacyTid;
		bestPos = info.bestPos;

		// Randomly pick a season that he played on this team, and use that for colors
		const teamJerseyNumberInfos = jerseyNumberInfos.filter(
			(info) => info.t && info.t.tid === legacyTid,
		);
		if (teamJerseyNumberInfos.length > 0) {
			const info = choice(teamJerseyNumberInfos);
			if (info.t) {
				teamColors = info.t.colors;
				teamJersey = info.t.jersey;
			}
		}
	} else {
		bestPos = p.ratings.at(-1)!.pos;
	}
	if (teamColors === undefined) {
		teamColors = await getTeamColors(p.tid);
	}
	if (teamJersey === undefined) {
		teamJersey = (await idb.cache.teams.get(p.tid))?.jersey ?? DEFAULT_JERSEY;
	}

	// Quick links to other players...
	let customMenu: MenuItemHeader | undefined;
	let customMenuInfo:
		| {
				title: string;
				players: Player[];
		  }
		| undefined;
	if (p.tid >= 0) {
		// ...on same team

		customMenuInfo = {
			title: "Roster",
			players: await idb.cache.players.indexGetAll("playersByTid", p.tid),
		};
	} else if (p.tid === PLAYER.FREE_AGENT) {
		// ...also free agents

		customMenuInfo = {
			title: "Free Agents",
			players: await idb.cache.players.indexGetAll("playersByTid", p.tid),
		};
	} else if (p.tid === PLAYER.UNDRAFTED) {
		// ...in same draft class

		customMenuInfo = {
			title: "Draft Class",
			players: (
				await idb.cache.players.indexGetAll("playersByTid", p.tid)
			).filter((p2) => p2.draft.year === p.draft.year),
		};
	}

	if (customMenuInfo) {
		const children: MenuItemLink[] = orderBy(
			customMenuInfo.players,
			"value",
			"desc",
		).map((p2) => {
			const ratings = p2.ratings.at(-1)!;

			const age = g.get("season") - p2.born.year;

			let description = `${age}yo`;

			if (!g.get("challengeNoRatings")) {
				// Assembled here rather than through playersPlus, so the coarse
				// rounding has to be applied by hand or this dropdown quietly shows
				// exact ratings in a league that hides them.
				const show = (value: number) => {
					const fuzzed = player.fuzzRating(value, ratings.fuzz);
					return g.get("hideRatingsOnesDigit") &&
						!exemptFromCoarseRatings(
							p2.tid,
							g.get("hideRatingsOnesDigitExceptProspects"),
						)
						? coarsenRating(fuzzed)
						: fuzzed;
				};

				description += `, ${show(ratings.ovr)}/${show(ratings.pot)}`;
			}

			const path = [view, p2.pid];
			if (season !== undefined) {
				path.push(season);
			}

			return {
				type: "link",
				league: true,
				path,
				text: `${ratings.pos} ${p2.firstName} ${p2.lastName} (${description})`,
			};
		});

		customMenu = {
			type: "header",
			long: customMenuInfo.title,
			short: customMenuInfo.title,
			league: true,
			children,
		};
	}

	const statSummary = Object.values(PLAYER_SUMMARY);

	let statTables;
	if (isSport("baseball") && (bestPos === "SP" || bestPos === "RP")) {
		// Primarily a pitcher, so show pitching stats first - keep in sync with playerGameLog.ts
		statTables = Object.keys(PLAYER_STATS_TABLES).map((type) => {
			if (type === "pitching") {
				return PLAYER_STATS_TABLES.batting!;
			}

			if (type === "batting") {
				return PLAYER_STATS_TABLES.pitching!;
			}

			return PLAYER_STATS_TABLES[type]!;
		});
	} else {
		statTables = Object.values(PLAYER_STATS_TABLES);
	}

	let teamURL;
	if (p.tid >= 0) {
		teamURL = helpers.leagueUrl(["roster", `${p.abbrev}_${p.tid}`]);
	} else if (p.tid === PLAYER.FREE_AGENT) {
		teamURL = helpers.leagueUrl(["free_agents"]);
	} else if (
		p.tid === PLAYER.UNDRAFTED ||
		p.tid === PLAYER.UNDRAFTED_FANTASY_TEMP
	) {
		teamURL = helpers.leagueUrl(["draft_scouting"]);
	}

	if (season !== undefined) {
		// Age/experience
		if (p.stats.length > 0) {
			const offset = season - g.get("season");
			p.age = Math.max(0, p.age + offset);
			const offset2 = season - p.stats.at(-1)!.season;
			p.experience = Math.max(0, p.experience + offset2);

			// Jersey number
			const stats = p.stats.findLast(
				(row) => row.season === season && !row.playoffs && row.tid >= 0,
			);
			if (stats) {
				if (stats.jerseyNumber !== undefined) {
					p.jerseyNumber = stats.jerseyNumber;
				}

				const info = await getTeamInfoBySeason(stats.tid, stats.season);
				if (info) {
					teamName = `${info.region} ${info.name}`;
					teamColors = info.colors;
					teamJersey = info.jersey;
				}

				teamURL = helpers.leagueUrl([
					"roster",
					`${stats.abbrev}_${stats.tid}`,
					season,
				]);
			}
		}
	}

	let randomDebutsForeverPids;
	if (g.get("randomDebutsForever") !== undefined && p.srID !== undefined) {
		randomDebutsForeverPids = [];
		for await (const { value: p2 } of idb.league
			.transaction("players")
			.store.index("srID")
			.iterate(p.srID)) {
			randomDebutsForeverPids.push(p2.pid);
		}

		// No point showing if there are no other versions
		if (randomDebutsForeverPids.length === 1) {
			randomDebutsForeverPids = undefined;
		}
	}

	// Names the note can link, scoped per season - only computed when there IS a
	// note, so an ordinary player page pays nothing for it.
	const noteTeammates = await getNoteTeammates(pRaw);

	// Every card anyone in the room has made of this player, newest first. The
	// store is fully cached, so this is an in-memory filter.
	const tradingCards = (await idb.cache.tradingCards.getAll())
		.filter((card) => card.pid === pid)
		.sort((a, b) => b.at - a.at);

	// What each season of this contract actually bought, priced against the
	// league in THAT season - a win cost something different in 2005 than it
	// does now, so every season is calibrated on its own. Only seasons he was
	// paid for and played in produce a number; future years of a deal have no
	// production to price yet, and come back undefined.
	const contractValues = new Map<number, ContractValueBreakdown>();
	if (isSport("basketball")) {
		const paidSeasons = (p.salaries ?? []).map((s: any) => s.season);
		const contexts = await loadContractValueContexts(paidSeasons);
		for (const salary of p.salaries ?? []) {
			const context = contexts.get(salary.season);
			if (!context) {
				continue;
			}
			const stats = (p.stats as any[]).findLast(
				(row) => row.season === salary.season && !row.playoffs,
			);
			const value = valueForPlayer(
				{ pid, salary: salary.amount, stats },
				context,
			);
			if (value) {
				contractValues.set(salary.season, value);
			}
		}
	}

	return {
		type: "normal" as const,
		contractValues: [...contractValues],
		bestPos,
		customMenu,
		tradingCards,
		appearanceTeams,
		jerseyNumberInfos,
		noteTeammates,
		pRaw,
		pid, // Needed for state.pid check
		player: p,
		randomDebutsForeverPids,
		retired,
		statSummary,
		statTables,
		teamColors,
		teamJersey,
		teamName,
		teamURL,
		willingToSign,
	};
};

const updatePlayer = async (
	inputs: ViewInput<"player">,
	updateEvents: UpdateEvents,
	state: any,
) => {
	if (
		updateEvents.includes("firstRun") ||
		updateEvents.includes("playerMovement") ||
		updateEvents.includes("tradingCards") ||
		!state.retired ||
		state.pid !== inputs.pid
	) {
		const topStuff = await getCommon(inputs.pid, undefined, "player");

		if (topStuff.type === "error") {
			// https://stackoverflow.com/a/59923262/786644
			const returnValue = {
				errorMessage: topStuff.errorMessage,
			};
			return returnValue;
		}

		const p = topStuff.player;

		const eventsAll = orderBy(
			[
				...(await idb.getCopies.events(
					{
						pid: topStuff.pid,
					},
					"noCopyCache",
				)),
				...(p.draft.dpid !== undefined
					? await idb.getCopies.events(
							{
								dpid: p.draft.dpid,
							},
							"noCopyCache",
						)
					: []),
			],
			"eid",
			"asc",
		);
		const feats = eventsAll
			.filter((event) => event.type === "playerFeat")
			.map((event) => {
				return {
					eid: event.eid,
					season: event.season,
					text: helpers.correctLinkLid(g.get("lid"), event.text as any),
				};
			});
		const eventsFiltered = eventsAll.filter((event) => {
			// undefined is a temporary workaround for bug from commit 999b9342d9a3dc0e8f337696e0e6e664e7b496a4
			return !(
				event.type === "award" ||
				event.type === "injured" ||
				event.type === "healed" ||
				event.type === "hallOfFame" ||
				event.type === "playerFeat" ||
				event.type === "tragedy" ||
				event.type === undefined
			);
		});

		const events = [];
		for (const event of eventsFiltered) {
			events.push({
				eid: event.eid,
				text: await formatEventText(event),
				season: event.season,
			});
		}

		const leaders = await player.getLeaders(topStuff.pRaw);

		return {
			...topStuff,
			events,
			feats,
			leaders,
			ratings: RATINGS,
		};
	}
};

export default updatePlayer;
