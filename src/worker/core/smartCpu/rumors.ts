import { PHASE } from "../../../common/constants.ts";
import type { EventBBGM, Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { eventDate } from "../../util/eventDate.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import type { MarketTrace, SmartDeal } from "./market.ts";
import type { Holdings } from "./worth.ts";

// THE RUMOR MILL (owner, 2026-11-27: "putting all this on the front end ...
// in some fun immersive way"). What the CPU teams really said to each other
// at a market moment, told the way the insiders tell it: who has called about
// whom, the offers turned down and the talks that came close. Only from the
// moment's own record (MarketTrace) - never a talk that didn't happen, no
// valuations, no quotes - and only CPU teams, as only they meet there. Saved
// as "rumor" events: the News Feed shows them, and a player's page while he
// is still where the talk left him. Presentation only: no Math.random (the
// sim's own stream is left as it was), nothing any team decides by.
export const rumorSettings = {
	// Stories a moment: on each of the deadline's last nights, at any other
	// moment in season, and in the offseason.
	deadlineNight: 3,
	inSeason: 1,
	offseason: 2,
	// Only about the league's best players by the game's value, this many.
	topPlayers: 150,
	// A player again only once his talks go further than the last story
	// about him, or with another team once that story is this many days old
	// (on classic days, this many stories about others back) - never the same
	// story twice in a season. Teams in that many days' stories count against
	// a new one, so the same busy team isn't every story.
	quietDays: 14,
	quietStories: 8,
	// Scored for the News Feed: a top player's story is bigger news.
	starRank: 30,
};

// How far a talk got: someone wanting him (1), an offer his team turned
// down (2), a deal both sides came close to (3).
type Tier = 1 | 2 | 3;
const TIER_OF: Partial<Record<MarketTrace["stage"], Tier>> = {
	"seller says no": 2,
	changed: 3,
	"dealt already": 3,
	"within margin": 3,
	unfair: 3,
	waits: 3,
};

type Talk = {
	p: Player;
	seller: number;
	// The teams that wanted him past what his team needs for him.
	bidders: readonly number[];
	tier: 0 | Tier;
	stage?: MarketTrace["stage"];
	buyer?: number;
	offer?: Holdings;
	sellerGain?: number;
};

// The moment's talks by player, while one is under way.
let talks: Map<number, Talk> | undefined;

export const beginTalks = () => {
	talks = new Map();
};

export const endTalks = () => {
	talks = undefined;
};

export const noteTalk = (t: MarketTrace) => {
	if (!talks) {
		return;
	}
	let talk = talks.get(t.p.pid);
	if (!talk) {
		talk = { p: t.p, seller: t.seller, bidders: [], tier: 0 };
		talks.set(t.p.pid, talk);
	}
	if (t.buyers) {
		talk.bidders = t.buyers;
	}
	const tier = TIER_OF[t.stage];
	if (tier !== undefined && tier >= talk.tier) {
		talk.tier = tier;
		talk.stage = t.stage;
		talk.buyer = t.buyer;
		talk.offer = t.offer;
		talk.sellerGain = t.sellerGain;
	}
};

// What a talk makes for a story: talks that came close (3), an offer his team
// nearly took (2), two or more teams after him (1), or an offer far short of
// what his team wants (1) - its offer left out, as a buyer's most for a
// player its team won't move makes no sense printed.
type Kind = "close" | "near" | "interest" | "apart";
const NEAR = 0.01;
const kindOf = (
	talk: Talk,
): { kind: Kind; tier: Tier; buyer?: number } | undefined => {
	if (talk.tier === 3 && talk.buyer !== undefined) {
		return { kind: "close", tier: 3, buyer: talk.buyer };
	}
	if (
		talk.tier === 2 &&
		talk.buyer !== undefined &&
		talk.offer &&
		(talk.sellerGain ?? -1) > -NEAR
	) {
		return { kind: "near", tier: 2, buyer: talk.buyer };
	}
	if (talk.bidders.length >= 2) {
		return { kind: "interest", tier: 1 };
	}
	if (talk.tier === 2 && talk.buyer !== undefined) {
		return { kind: "apart", tier: 1, buyer: talk.buyer };
	}
	return undefined;
};

// One of a few ways to say it, the same for the same player and day - not
// Math.random, which would move the sim's own stream.
const variant = <T>(xs: readonly T[], seed: number) =>
	xs[Math.abs(seed) % xs.length]!;

const teamInfo = (tid: number) => g.get("teamInfoCache")[tid];

// "the <a>Trail Blazers</a>", capitalized to start a sentence.
const team = (tid: number, start = false) => {
	const t = teamInfo(tid);
	if (!t) {
		return "???";
	}
	return `${start ? "The" : "the"} <a href="${helpers.leagueUrl(["roster", `${t.abbrev}_${tid}`, g.get("season")])}">${t.name}</a>`;
};

// "the <a>Clippers</a>'", "the <a>Jazz</a>'s".
const teamsOwn = (tid: number) =>
	`${team(tid)}${teamInfo(tid)?.name.endsWith("s") ? "'" : "'s"}`;

const playerLink = (p: Player) =>
	`<a href="${helpers.leagueUrl(["player", p.pid])}">${p.firstName} ${p.lastName}</a>`;

const count = (k: number, word: string) =>
	`${["", "a", "two", "three", "four", "five"][k] ?? k} ${word}${k > 1 ? "s" : ""}`;

const listOf = (parts: string[]) =>
	parts.length <= 1
		? (parts[0] ?? "")
		: `${parts.slice(0, -1).join(", ")} and ${parts.at(-1)}`;

// An offer in words: its players by name (the two best, then how many
// more), its picks counted by round.
const offerPhrase = (offer: Holdings) => {
	const players = [...offer.players].sort((a, b) => b.value - a.value);
	const parts = players.slice(0, 2).map(playerLink);
	if (players.length === 3) {
		parts.push("another player");
	} else if (players.length > 3) {
		parts.push(count(players.length - 2, "other player"));
	}
	const firsts = offer.picks.filter((dp) => dp.round === 1).length;
	const seconds = offer.picks.length - firsts;
	if (firsts > 0) {
		parts.push(count(firsts, "first-round pick"));
	}
	if (seconds > 0) {
		parts.push(count(seconds, "second-round pick"));
	}
	return listOf(parts);
};

const story = (talk: Talk, kind: Kind, seed: number) => {
	const p = playerLink(talk.p);
	const S = team(talk.seller, true);
	const s = team(talk.seller);
	if (kind === "interest") {
		const named = talk.bidders.slice(0, 2).map((tid) => team(tid));
		return variant(
			[
				`${p} is drawing interest from ${talk.bidders.length} teams, ${listOf(named)} among them, league sources say.`,
				`At least ${talk.bidders.length} teams have called ${s} about ${p}, per sources, including ${listOf(named)}.`,
			],
			seed,
		);
	}
	const B = team(talk.buyer!, true);
	const b = team(talk.buyer!);
	const offer = talk.offer ? offerPhrase(talk.offer) : "";
	if (kind === "apart") {
		return variant(
			[
				`${B} have asked ${s} about ${p}, sources say, but the two sides are far apart.`,
				`${B} checked in with ${s} on ${p}, per league sources. ${S} want a lot more than what's been offered.`,
			],
			seed,
		);
	}
	if (kind === "near") {
		return variant(
			[
				`${S} turned down an offer of ${offer} from ${b} for ${p}, league sources say. The two sides aren't far apart.`,
				`${B} offered ${offer} for ${p}, per sources. ${S} said no for now, but talks could pick back up.`,
				`${S} passed on ${teamsOwn(talk.buyer!)} offer of ${offer} for ${p}, a source familiar with the talks said, but the gap is small.`,
			],
			seed,
		);
	}
	if (talk.stage === "unfair") {
		return variant(
			[
				`${S} are holding out for more from ${b} for ${p}, according to a source familiar with the talks.${offer ? ` ${B} have offered ${offer}.` : ""}`,
				`${B} have made a run at ${p}${offer ? `, offering ${offer}` : ""}, league sources say, but ${s} want more.`,
			],
			seed,
		);
	}
	if (talk.stage === "waits") {
		return variant(
			[
				`${B} and ${s} have talked about ${p}, sources say, but both sides want to see who else comes available in the coming weeks.`,
				`${p} has come up in talks between ${b} and ${s}, per league sources. Neither side is in a hurry.`,
			],
			seed,
		);
	}
	return variant(
		[
			`${B} and ${s} have discussed a deal for ${p}, league sources say.${offer ? ` The framework has ${offer} going to ${s}.` : ""} Neither side is ready to pull the trigger yet.`,
			`${B} are in talks with ${s} on ${p}, per sources${offer ? `, with ${offer} on the table` : ""}. Nothing is done.`,
			`${p} has been the subject of trade talks between ${s} and ${b}, a source familiar with the talks said.${offer ? ` ${B} have put ${offer} on the table.` : ""}`,
		],
		seed,
	);
};

// The days from one "YYYY-MM-DD" to another.
const daysBetween = (from: string, to: string) =>
	Math.round(
		(Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) /
			86_400_000,
	);

// Tell the moment's best stories, about players still where the talk left
// them (a player traded this moment has the trade's own news).
export const tellRumors = async (made: readonly SmartDeal[]) => {
	const now = talks;
	if (!now || now.size === 0) {
		return;
	}
	const phase = g.get("phase");
	const inSeason = phase === PHASE.REGULAR_SEASON;
	const deadlineNight =
		inSeason && (g.get("smartMarketMoment") ?? "").includes(":deadline:");
	const limit = deadlineNight
		? rumorSettings.deadlineNight
		: inSeason
			? rumorSettings.inSeason
			: rumorSettings.offseason;
	const traded = new Set(made.flatMap((d) => d.out.players.map((p) => p.pid)));
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);
	const rank = new Map(
		[...players]
			.sort((a, b) => b.value - a.value)
			.map((p, i) => [p.pid, i + 1]),
	);
	const season = g.get("season");
	const told = (await idb.cache.events.getAll())
		.filter(
			(e): e is EventBBGM & { rumor: { tier: number; buyer?: number } } =>
				e.type === "rumor" && e.season === season && e.rumor !== undefined,
		)
		.sort((a, b) => a.eid - b.eid);
	const today = await eventDate();
	// Whether a story told then is still recent.
	const recent = (e: EventBBGM, i: number) =>
		today !== undefined && e.date !== undefined
			? daysBetween(e.date, today) < rumorSettings.quietDays
			: told.length - 1 - i < rumorSettings.quietStories;
	// A player again only once his talks go further, or with another team
	// once the last story about him is old; never the same story twice.
	const quiet = (pid: number, tier: number, buyer: number | undefined) =>
		told.some(
			(e, i) =>
				e.pids?.[0] === pid &&
				tier <= e.rumor.tier &&
				(e.rumor.buyer === buyer || recent(e, i)),
		);
	// Teams in the news lately, so the same busy team isn't every story.
	const lately = new Map<number, number>();
	for (const [i, e] of told.entries()) {
		if (recent(e, i)) {
			for (const tid of e.tids ?? []) {
				lately.set(tid, (lately.get(tid) ?? 0) + 1);
			}
		}
	}
	const picked = [...now.values()]
		.filter(
			(talk) =>
				!traded.has(talk.p.pid) &&
				talk.p.tid === talk.seller &&
				(rank.get(talk.p.pid) ?? Infinity) <= rumorSettings.topPlayers,
		)
		.flatMap((talk) => {
			const kind = kindOf(talk);
			if (!kind || quiet(talk.p.pid, kind.tier, kind.buyer)) {
				return [];
			}
			const tids =
				kind.buyer !== undefined
					? [kind.buyer, talk.seller]
					: [...talk.bidders.slice(0, 2), talk.seller];
			return [
				{
					talk,
					...kind,
					tids,
					// A talk that got further is bigger news, and so is a better
					// player; a team already in the news less so.
					weight:
						talk.p.value +
						4 * kind.tier -
						4 * tids.reduce((sum, tid) => sum + (lately.get(tid) ?? 0), 0),
				},
			];
		})
		.sort((a, b) => b.weight - a.weight || a.talk.p.pid - b.talk.p.pid);
	const teamsTold = new Set<number>();
	let n = 0;
	for (const { talk, kind, tier, buyer, tids } of picked) {
		if (n >= limit) {
			break;
		}
		// One story a team a moment.
		if (tids.some((tid) => teamsTold.has(tid))) {
			continue;
		}
		for (const tid of tids) {
			teamsTold.add(tid);
		}
		await logEvent({
			type: "rumor",
			text: story(talk, kind, talk.p.pid + season + told.length + n),
			pids: [talk.p.pid],
			tids,
			score:
				(rank.get(talk.p.pid) ?? Infinity) <= rumorSettings.starRank ? 15 : 10,
			rumor: { tier, ...(buyer === undefined ? {} : { buyer }) },
			showNotification: false,
		});
		n += 1;
	}
};
