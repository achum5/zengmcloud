import { PHASE } from "../../common/constants.ts";
import { draft, finances, team } from "../core/index.ts";
import { idb } from "../db/index.ts";
import { g, helpers } from "../util/index.ts";
import type {
	TeamSeason,
	UpdateEvents,
	ViewInput,
} from "../../common/types.ts";
import { getAutoTicketPriceByTid } from "../core/game/attendance.ts";
import addFirstNameShort from "../util/addFirstNameShort.ts";
import {
	getProjectedContractAmounts,
	projectNextContract,
} from "../util/projectedContracts.ts";
import { processDraftPicks } from "./draftPicks.ts";
import {
	loadContractValueContext,
	valueForPlayer,
} from "../util/contractValues.ts";

const updateTeamFinances = async (
	inputs: ViewInput<"teamFinances">,
	updateEvents: UpdateEvents,
	state: any,
) => {
	if (
		updateEvents.includes("firstRun") ||
		updateEvents.includes("gameSim") ||
		updateEvents.includes("playerMovement") ||
		updateEvents.includes("teamFinances") ||
		inputs.tid !== state.tid ||
		inputs.show !== state.show
	) {
		const contractsRaw = await team.getContracts(inputs.tid);
		let payroll = await team.getPayroll(contractsRaw);
		const luxuryTaxAmount = finances.getLuxuryTaxAmount(payroll) / 1000;
		const minPayrollAmount = finances.getMinPayrollAmount(payroll) / 1000;
		payroll /= 1000;

		let showInt;

		if (inputs.show === "all") {
			showInt = g.get("season") - g.get("startingSeason") + 1;
		} else {
			showInt = Number.parseInt(inputs.show);
		}

		let season = g.get("season");
		if (g.get("phase") >= PHASE.DRAFT) {
			// After the draft, don't show old contract year
			season += 1;
		}

		// How many seasons into the future are contracts?
		let maxContractExp = -Infinity;
		for (const contract of contractsRaw) {
			if (contract.exp > maxContractExp) {
				maxContractExp = contract.exp;
			}
		}
		const numSeasons = Math.max(
			g.get("maxContractLength"),
			maxContractExp - season + 1,
		);

		// What a player would cost to keep once his current deal runs out. The
		// table otherwise just goes blank after a contract expires, which reads as
		// "free" when it usually means "the biggest bill coming". These are
		// projections, not commitments, so they are marked with an asterisk in the
		// UI and deliberately left OUT of the totals and cap space rows below -
		// those stay strictly what the team has actually committed.
		const projectedAmounts = await getProjectedContractAmounts();
		const rosterByPid = new Map(
			(await idb.cache.players.indexGetAll("playersByTid", inputs.tid)).map(
				(p) => [p.pid, p],
			),
		);
		const lastSeasonShown = season + numSeasons - 1;

		// How much of this season's production each contract has bought so far,
		// priced against the rest of the league (see util/contractValues.ts). This
		// page only has one team loaded, so unlike the player tables it has to go
		// and read the league to calibrate.
		const contractValueContext = await loadContractValueContext(
			g.get("season"),
		);
		const currentSeasonStats = (pid: number) => {
			const p = rosterByPid.get(pid);
			return p?.stats.findLast(
				(row: any) => row.season === g.get("season") && !row.playoffs,
			);
		};

		// Convert contract objects into table rows
		const contractTotals = Array(numSeasons).fill(0);
		const contracts = addFirstNameShort(
			contractsRaw.map((contract) => {
				const amounts: number[] = [];

				for (let i = season; i <= contract.exp; i++) {
					amounts.push(contract.amount / 1000);
					if (contractTotals[i - season] !== undefined) {
						contractTotals[i - season] += contract.amount / 1000;
					}
				}

				// Released players are gone - there is no next contract to project,
				// only the dead money already in `amounts`.
				const projected: (number | undefined)[] =
					Array(numSeasons).fill(undefined);
				const p = rosterByPid.get(contract.pid);
				if (p && !contract.released && contract.exp < lastSeasonShown) {
					const next = projectNextContract(p, projectedAmounts);
					const start = Math.max(season, contract.exp + 1);
					for (
						let i = start;
						i < start + next.years && i <= lastSeasonShown;
						i++
					) {
						projected[i - season] = next.amount / 1000;
					}
				}

				return {
					pid: contract.pid,
					firstName: contract.firstName,
					lastName: contract.lastName,
					skills: contract.skills,
					pos: contract.pos,
					injury: contract.injury,
					jerseyNumber: contract.jerseyNumber,
					watch: contract.watch,
					released: contract.released,
					amounts,
					amountsProjected: projected,
					capPct: (100 * contract.amount) / g.get("salaryCap"),
					// Released players are excluded on purpose: dead money bought no
					// production at all, so "was it good value" is not a question
					// with an answer, and a big negative would just be noise.
					contractValue: contract.released
						? undefined
						: valueForPlayer(
								{
									pid: contract.pid,
									salary: contract.amount / 1000,
									stats: currentSeasonStats(contract.pid),
								},
								contractValueContext,
							)?.surplus,
				};
			}),
		);

		const salariesSeasons = [];
		for (let i = 0; i < numSeasons; i++) {
			salariesSeasons.push(season + i);
		}

		// The picks this team owns, priced at the rookie scale.
		//
		// A pick is money the team has all but committed - the salary is fixed by
		// the slot the moment it's used - but it isn't on the books yet, so it's
		// presented the same way an expiring player's next deal is: shown, marked,
		// and left out of the committed totals unless it's checked in.
		//
		// The draft is held AFTER its season ends, so a pick in draft year Y first
		// costs money in Y+1.
		const rookieSalaries = draft.getRookieSalaries();
		const numActiveTeams = g.get("numActiveTeams");
		const draftPicksRaw = await idb.cache.draftPicks.indexGetAll(
			"draftPicksByTid",
			inputs.tid,
		);
		const draftPicks = (await processDraftPicks(draftPicksRaw))
			.map((dp) => {
				// A pick's number is known once the order is set; before that the
				// Draft Picks page's projection is the best estimate there is, and
				// it's what the slot money has to be read off.
				const pickInRound = dp.pick > 0 ? dp.pick : dp.projectedPick;
				const slot =
					pickInRound === undefined
						? undefined
						: (dp.round - 1) * numActiveTeams + pickInRound;
				const amount =
					slot === undefined ? undefined : rookieSalaries[slot - 1];

				const amounts: (number | undefined)[] =
					Array(numSeasons).fill(undefined);
				if (amount !== undefined && typeof dp.season === "number") {
					const start = dp.season + 1;
					const length = draft.getRookieContractLength(dp.round);
					for (let year = start; year < start + length; year++) {
						const i = year - season;
						if (i >= 0 && i < numSeasons) {
							amounts[i] = amount / 1000;
						}
					}
				}

				return {
					dpid: dp.dpid,
					season: dp.season,
					round: dp.round,
					pick: dp.pick,
					projectedPick: dp.projectedPick,
					originalAbbrev: dp.originalAbbrev,
					originalTid: dp.originalTid,
					slot,
					amount: amount === undefined ? undefined : amount / 1000,
					capPct:
						amount === undefined ? 0 : (100 * amount) / g.get("salaryCap"),
					amounts,
				};
			})
			// A pick whose rookie deal starts past the last column has nothing to
			// show in this table; the Draft Picks page has the full list.
			.filter((dp) => dp.amounts.some((x) => x !== undefined))
			.sort(
				(a, b) =>
					(typeof a.season === "number" ? a.season : Infinity) -
						(typeof b.season === "number" ? b.season : Infinity) ||
					a.round - b.round ||
					(a.slot ?? Infinity) - (b.slot ?? Infinity),
			);

		// The cap moves. In a real-players league scheduled events step the salary
		// cap, luxury tax and hard cap up every season, so measuring a 2030 column
		// against today's cap is wrong by a lot by the time you get there.
		// Processed events are deleted, so everything still in the store is in the
		// future - walk them forward, season by season, to get the caps each
		// column should actually be judged against.
		const running = {
			salaryCap: g.get("salaryCap"),
			luxuryPayroll: g.get("luxuryPayroll"),
			minPayroll: g.get("minPayroll"),
			hardCapAmount: g.get("hardCapAmount"),
			hardCapTids: g.get("hardCapTids"),
			hardCapUseLuxuryTax: g.get("hardCapUseLuxuryTax"),
		};
		const gameAttributeEvents = (await idb.cache.scheduledEvents.getAll())
			.filter((event) => event.type === "gameAttributes")
			.sort((a, b) => a.season - b.season || a.phase - b.phase);
		let eventIndex = 0;
		const capsBySeason = salariesSeasons.map((yr) => {
			while (
				eventIndex < gameAttributeEvents.length &&
				gameAttributeEvents[eventIndex]!.season <= yr
			) {
				const info = (gameAttributeEvents[eventIndex] as any).info ?? {};
				for (const key of helpers.keys(running)) {
					if (info[key] !== undefined) {
						(running as any)[key] = info[key];
					}
				}
				eventIndex += 1;
			}
			return { ...running };
		});

		const teamSeasons = await idb.getCopies.teamSeasons({
			tid: inputs.tid,
		});
		teamSeasons.reverse(); // Most recent season first

		// Add in luxuryTaxShare if it's missing
		for (const teamSeason of teamSeasons) {
			if (!teamSeason.revenues.luxuryTaxShare) {
				teamSeason.revenues.luxuryTaxShare = 0;
			}
		}

		const formatRevenueExpenses = (teamSeason: TeamSeason) => {
			const output = {} as Record<
				| `expenses${Capitalize<keyof TeamSeason["expenses"]>}`
				| `revenues${Capitalize<keyof TeamSeason["revenues"]>}`,
				number
			>;
			for (const key of helpers.keys(teamSeason.revenues)) {
				const outputKey = `revenues${helpers.upperCaseFirstLetter(
					key,
				)}` as const;
				output[outputKey] = teamSeason.revenues[key];
			}
			for (const key of helpers.keys(teamSeason.expenses)) {
				const outputKey = `expenses${helpers.upperCaseFirstLetter(
					key,
				)}` as const;
				output[outputKey] = teamSeason.expenses[key];
			}
			return output;
		};

		const barData = teamSeasons.slice(0, showInt).map((teamSeason) => {
			const att = teamSeason.att / teamSeason.gpHome;

			const numPlayoffRounds = g.get(
				"numGamesPlayoffSeries",
				teamSeason.season,
			).length;

			const champ = teamSeason.playoffRoundsWon === numPlayoffRounds;

			const row = {
				season: teamSeason.season,
				champ,
				att,
				cash: teamSeason.cash / 1000, // convert to millions
				won: teamSeason.won,
				hype: teamSeason.hype,
				pop: teamSeason.pop,
				...formatRevenueExpenses(teamSeason),
			};

			return row;
		});

		// Pad with 0s
		while (barData.length > 0 && barData.length < showInt) {
			const row = helpers.deepCopy(barData.at(-1)!);
			row.season -= 1;
			for (const key of helpers.keys(row)) {
				if (key !== "season" && key !== "champ") {
					row[key] = 0;
				}
			}
			barData.push(row);
		}

		// Get stuff for the finances form
		const tTemp = await idb.getCopy.teamsPlus(
			{
				attrs: ["budget", "adjustForInflation", "autoTicketPrice"],
				seasonAttrs: ["expenses"],
				season: g.get("season"),
				tid: inputs.tid,
				addDummySeason: true,
			},
			"noCopyCache",
		);

		if (!tTemp) {
			throw new Error("Team not found");
		}

		const t = tTemp as typeof tTemp & {
			autoTicketPrice: boolean;
			expenseLevelsLastThree: TeamSeason["expenseLevels"];
		};

		// undefined is true (for upgrades), and AI teams are always true
		t.autoTicketPrice =
			t.autoTicketPrice !== false || !g.get("userTids").includes(inputs.tid);

		// Undo reverse from above
		const teamSeasonsLastThree = teamSeasons.slice(0, 3).reverse();
		t.expenseLevelsLastThree = {
			coaching: await finances.getLevelLastThree("coaching", {
				tid: inputs.tid,
				teamSeasons: teamSeasonsLastThree,
			}),
			facilities: await finances.getLevelLastThree("facilities", {
				tid: inputs.tid,
				teamSeasons: teamSeasonsLastThree,
			}),
			health: await finances.getLevelLastThree("health", {
				tid: inputs.tid,
				teamSeasons: teamSeasonsLastThree,
			}),
			scouting: await finances.getLevelLastThree("scouting", {
				tid: inputs.tid,
				teamSeasons: teamSeasonsLastThree,
			}),
		};

		const maxStadiumCapacity = teamSeasons.reduce((max, teamSeason) => {
			if (teamSeason.stadiumCapacity > max) {
				return teamSeason.stadiumCapacity;
			}

			return max;
		}, 0);

		const autoTicketPrice = await getAutoTicketPriceByTid(inputs.tid);

		const otherTeamTicketPrices = [];
		const teams = await idb.cache.teams.getAll();
		for (const t of teams) {
			if (!t.disabled && t.tid !== inputs.tid) {
				if (t.autoTicketPrice) {
					otherTeamTicketPrices.push(await getAutoTicketPriceByTid(t.tid));
				} else {
					otherTeamTicketPrices.push(t.budget.ticketPrice);
				}
			}
		}
		otherTeamTicketPrices.sort((a, b) => b - a);

		return {
			abbrev: inputs.abbrev,
			autoTicketPrice,
			tid: inputs.tid,
			show: inputs.show,
			minPayrollAmount,
			luxuryTaxAmount,
			maxStadiumCapacity,
			t,
			barData,
			payroll,
			contracts,
			draftPicks,
			contractTotals,
			salariesSeasons,
			capsBySeason,
			otherTeamTicketPrices,
		};
	}
};

// Which rows the salary table is counting for this team. Cosmetic, cheap, and
// deliberately outside updateTeamFinances's guard: it has to arrive on the
// first render, and it costs nothing to hand over every time.
//
// Only ever for the team THIS DEVICE is playing. In a multiplayer league
// everyone runs multi-team mode, so every human team is in userTids and any
// check looser than this would put a league-mate's cap sheet on your screen
// with their ticks in it. Every other team - human or not - reads as a plain
// CPU team here.
const updatePlan = (inputs: ViewInput<"teamFinances">) => ({
	plan:
		inputs.tid === g.get("userTid")
			? g.get("teamFinancesPlan")[inputs.tid]
			: undefined,
});

export default async (
	inputs: ViewInput<"teamFinances">,
	updateEvents: UpdateEvents,
	state: any,
) => {
	return Object.assign(
		{},
		await updateTeamFinances(inputs, updateEvents, state),
		updatePlan(inputs),
	);
};
