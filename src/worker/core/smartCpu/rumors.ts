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
	// Only about the league's best players by the game's value, as far down
	// as each kind of story reaches: talks that came close or an offer nearly
	// taken are news about a rotation player too; teams after a player, about
	// a starter; a team asking about one far out of its reach, about a star
	// alone (in the owner's league, half the stories had been that, about
	// rotation players, every one alike).
	topPlayers: {
		close: 250,
		near: 200,
		available: 150,
		interest: 120,
		apart: 40,
	},
	// A player again only once his talks go further than the last story
	// about him, or with another team once that story is this many days old
	// (on classic days, this many stories about others back) - never the same
	// story twice in a season. Teams in that many days' stories count against
	// a new one, so the same busy team isn't every story.
	quietDays: 14,
	quietStories: 8,
	// Scored for the News Feed: a top player's story is bigger news.
	starRank: 30,
	// A team after at least this many players at a moment is a story of its
	// own, named with its two best targets among the league's this many best:
	// one such story a while (quietDays), a team once a season (in the
	// owner's league, some team was after six or more at every moment, and a
	// third of the stories had been that, about bench men).
	busyTeam: 6,
	busyTargets: 150,
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
	// His deal ends, his team isn't contending and he isn't staying (plans.ts).
	mustMove: boolean;
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
		talk = { p: t.p, seller: t.seller, bidders: [], tier: 0, mustMove: false };
		talks.set(t.p.pid, talk);
	}
	if (t.buyers) {
		talk.bidders = t.buyers;
	}
	if (t.mustMove) {
		talk.mustMove = true;
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
// nearly took (2), his team open to moving him with his deal ending (1), two
// or more teams after him (1), or an offer far short of what his team wants
// (1) - its offer left out, as a buyer's most for a player its team won't
// move makes no sense printed.
type Kind = "close" | "near" | "available" | "interest" | "apart";
// Which comes first when there's room for only some: the furthest talks,
// and a team asking far out of its reach last.
const RANK_OF: Record<Kind | "team", number> = {
	close: 3,
	near: 2,
	available: 1.5,
	team: 1.5,
	interest: 1,
	apart: 0,
};
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
	if (talk.mustMove) {
		return { kind: "available", tier: 1 };
	}
	if (talk.bidders.length >= 2) {
		return { kind: "interest", tier: 1 };
	}
	if (talk.tier === 2 && talk.buyer !== undefined) {
		return { kind: "apart", tier: 1, buyer: talk.buyer };
	}
	return undefined;
};

// How a story is told: an opening, who says so, then an ending. Openings go
// in turn through their kind's (seed.kind, the stories of that kind told so
// far this season), endings in turn against them, so the same pair comes
// back only after every opening has been used with others; who says so goes
// down its own list story by story (seed.all), so two in a row never source
// the same way. Not Math.random, which would move the sim's own stream (the
// owner, 2026-11-27: "build out the rumors wording so it's never too
// repetitive").
type Seed = { kind: number; all: number };

const SOURCES = [
	"league sources say",
	"per league sources",
	"sources say",
	"a source familiar with the talks said",
	"according to league sources",
	"people around the league say",
	"a league source said",
	"per sources",
] as const;

// An opening, with the endings only it can take (ones that finish what it
// starts), or the kind's own.
type Line = string | { lead: string; ends: readonly string[] };

const tell = (lines: readonly Line[], ends: readonly string[], seed: Seed) => {
	const k = Math.abs(seed.kind);
	const line = lines[k % lines.length]!;
	const lead = typeof line === "string" ? line : line.lead;
	const own = typeof line === "string" ? ends : line.ends;
	const round = Math.floor(k / lines.length);
	const end = own.length > 0 ? own[(k + round) % own.length]! : "";
	return `${lead}, ${SOURCES[Math.abs(seed.all) % SOURCES.length]}.${end}`;
};

// Every pairing of two lists of endings, each list moving on each time.
const mixed = (firsts: readonly string[], thens: readonly string[]) =>
	Array.from(
		{ length: firsts.length * thens.length },
		(_, i) =>
			firsts[i % firsts.length]! +
			thens[(i + Math.floor(i / firsts.length)) % thens.length]!,
	);

const cap = (text: string) => text.charAt(0).toUpperCase() + text.slice(1);

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

const numberWord = (k: number) =>
	[
		"no",
		"one",
		"two",
		"three",
		"four",
		"five",
		"six",
		"seven",
		"eight",
		"nine",
		"ten",
		"eleven",
	][k] ?? String(k);

// A team's calls counted loosely, as the insiders do: "at least eight",
// "more than a dozen", "dozens of".
const howMany = (k: number) =>
	k < 12
		? `at least ${numberWord(k)}`
		: k < 24
			? "more than a dozen"
			: "dozens of";

const count = (k: number, word: string) =>
	`${k === 1 ? "a" : numberWord(k)} ${word}${k > 1 ? "s" : ""}`;

const listOf = (parts: string[]) =>
	parts.length <= 1
		? (parts[0] ?? "")
		: `${parts.slice(0, -1).join(", ")} and ${parts.at(-1)}`;

// An offer in words: its players by name, its picks counted by round. One of
// many players is "a package built around" its two best ("Kostas
// Antetokounmpo, Kyle Kuzma, four other players and two first-round picks"
// read like a list, the same list three times in a month).
const offerPhrase = (offer: Holdings) => {
	const players = [...offer.players].sort((a, b) => b.value - a.value);
	const firsts = offer.picks.filter((dp) => dp.round === 1).length;
	const seconds = offer.picks.length - firsts;
	const picks = [
		...(firsts > 0 ? [count(firsts, "first-round pick")] : []),
		...(seconds > 0 ? [count(seconds, "second-round pick")] : []),
	];
	if (players.length > 3) {
		const core = `a package built around ${listOf(players.slice(0, 2).map(playerLink))}`;
		return picks.length > 0 ? `${core}, with ${listOf(picks)}` : core;
	}
	return listOf([...players.map(playerLink), ...picks]);
};

const story = (talk: Talk, kind: Kind, seed: Seed) => {
	const p = playerLink(talk.p);
	const S = team(talk.seller, true);
	const s = team(talk.seller);
	const k = talk.bidders.length;
	const N = numberWord(k);
	const [first, second] = talk.bidders;
	// The first two teams after him: "the Bucks and the Magic".
	const two =
		first !== undefined && second !== undefined
			? {
					start: `${team(first, true)} and ${team(second)}`,
					mid: `${team(first)} and ${team(second)}`,
				}
			: undefined;

	if (kind === "available") {
		const callers =
			first === undefined
				? []
				: two === undefined
					? [
							` ${team(first, true)} have called.`,
							` ${team(first, true)} have shown interest.`,
							` ${team(first, true)} have checked in.`,
						]
					: k === 2
						? [
								` ${two.start} have both called.`,
								` ${two.start} have both shown interest.`,
								` ${two.start} have both checked in.`,
							]
						: [
								` ${two.start} are among the teams that have called.`,
								` ${two.start} are among the teams interested.`,
								` ${cap(N)} teams have checked in, ${two.mid} among them.`,
							];
		return tell(
			[
				`${S} are open to moving ${p}, whose contract is up after the season`,
				`${S} are listening to offers for ${p}, with his deal running out`,
				`${S} aren't expected to re-sign ${p}, and are taking calls on him`,
				`${p} is available, with ${s} not expected to keep him past this season`,
				`${S} are shopping ${p} before his contract runs out`,
			],
			callers,
			seed,
		);
	}

	if (kind === "interest" && two) {
		return tell(
			k === 2
				? [
						`${p} is drawing interest from ${two.mid}`,
						`${two.start} have both called ${s} about ${p}`,
						`${S} have heard from ${two.mid} on ${p}`,
						`${two.start} have both asked about ${p}`,
						`${p} has caught the eye of ${two.mid}`,
						`${two.start} are both interested in ${p}`,
					]
				: [
						`${p} is drawing interest from ${N} teams, ${two.mid} among them`,
						`At least ${N} teams have called ${s} about ${p}, including ${two.mid}`,
						`${S} have heard from ${N} teams on ${p}, ${two.mid} among them`,
						`${p} has become one of the more popular names on the market, with ${team(first!)}, ${team(second!)} and others calling`,
						`${cap(N)} teams have asked ${s} about ${p}, ${two.mid} among them`,
						`${two.start} are among ${N} teams that have checked in on ${p}`,
					],
			[],
			seed,
		);
	}

	const buyer = talk.buyer!;
	const B = team(buyer, true);
	const b = team(buyer);
	const offer = talk.offer ? offerPhrase(talk.offer) : "";

	if (kind === "apart" || kind === "interest") {
		return tell(
			[
				`${B} have asked ${s} about ${p}, but the two sides are far apart`,
				{
					lead: `${B} checked in with ${s} on ${p}`,
					ends: [` ${S} want a lot more than what's been offered.`],
				},
				`${B} have shown interest in ${p}, but ${s} aren't close to moving him`,
				{
					lead: `${B} made an offer for ${p}`,
					ends: [` It wasn't close to what ${s} want.`],
				},
				{
					lead: `${B} asked about ${p}`,
					ends: [` ${S} weren't interested at the price.`],
				},
				`${B} have called ${s} about ${p}, without much luck`,
			],
			[],
			seed,
		);
	}

	if (kind === "near") {
		return tell(
			[
				`${S} turned down an offer of ${offer} from ${b} for ${p}`,
				`${B} offered ${offer} for ${p}, but ${s} said no`,
				`${S} passed on ${teamsOwn(buyer)} offer of ${offer} for ${p}`,
				`${S} said no when ${b} offered ${offer} for ${p}`,
				`${B} came up just short in a bid for ${p}, offering ${offer}`,
				`${B} put ${offer} on the table for ${p}, and ${s} said no`,
			],
			[
				" The two sides aren't far apart.",
				" The gap between them is small.",
				` ${B} could come back with more.`,
				" Talks could pick back up.",
				" The door isn't closed.",
				"",
			],
			seed,
		);
	}

	const offerEnds = (lines: readonly string[]) => (offer ? lines : [""]);

	if (talk.stage === "unfair") {
		return tell(
			[
				`${S} are holding out for more from ${b} for ${p}`,
				`${B} have made a run at ${p}, but ${s} want more`,
				`${S} are pushing ${b} to raise their offer for ${p}`,
				`${B} and ${s} are close on ${p}, but the price isn't settled`,
				`${S} want a little more from ${b} before they move ${p}`,
			],
			offerEnds([
				` ${B} have offered ${offer}.`,
				` The offer on the table is ${offer}.`,
				` So far ${b} have offered ${offer}.`,
			]),
			seed,
		);
	}

	if (talk.stage === "waits") {
		return tell(
			[
				`${B} and ${s} have talked about ${p}, but both sides want to see who else comes available in the coming weeks`,
				{
					lead: `${p} has come up in talks between ${b} and ${s}`,
					ends: [" Neither side is in a hurry."],
				},
				{
					lead: `${B} and ${s} have kept talks on ${p} going`,
					ends: [" Neither side is in a rush."],
				},
				`${B} and ${s} have the outline of a deal for ${p}, but are waiting to see what else the market brings`,
				`Talks between ${b} and ${s} on ${p} have slowed, with both sides keeping their options open`,
			],
			[],
			seed,
		);
	}

	const closers = [
		" Neither side is ready to pull the trigger yet.",
		" Nothing is done.",
		" Nothing is final.",
		"",
	];
	return tell(
		[
			`${B} and ${s} have discussed a deal for ${p}`,
			`${B} are in talks with ${s} on ${p}`,
			`${p} has been the subject of trade talks between ${s} and ${b}`,
			`${B} and ${s} are deep in talks on ${p}`,
			`${B} came close to a deal with ${s} for ${p}`,
			`${S} and ${b} have made progress on a deal for ${p}`,
		],
		offer
			? mixed(
					[
						` The framework has ${offer} going to ${s}.`,
						` ${B} have put ${offer} on the table.`,
						` ${S} would get ${offer}.`,
						` The deal being discussed sends ${offer} to ${s}.`,
					],
					closers,
				)
			: closers,
		seed,
	);
};

// A team after many players: "The <a>Heat</a> have been one of the league's
// most active teams ...".
const teamStory = (
	tid: number,
	n: number,
	targets: readonly Player[],
	seed: Seed,
) => {
	const B = team(tid, true);
	const b = team(tid);
	const names = listOf(targets.map(playerLink));
	const H = howMany(n);
	const when =
		g.get("phase") === PHASE.REGULAR_SEASON
			? "ahead of the deadline"
			: "this offseason";
	return tell(
		[
			`${B} have been one of the league's most active teams ${when}, checking in on ${H} players, ${names} among them`,
			`${B} are working the phones ${when}, with calls in on ${H} players, including ${names}`,
			{
				lead: `${names} ${targets.length > 1 ? "are" : "is"} among the players ${b} have asked about ${when}`,
				ends: [` They have called on ${H} players in all.`],
			},
			`${B} have checked in on ${H} players ${when}, ${names} among them`,
			`Few teams have been busier than ${b} ${when}. They have asked about ${H} players, including ${names}`,
			`${B} have cast a wide net ${when}, asking about ${H} players, ${names} among them`,
		],
		[],
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
			(
				e,
			): e is EventBBGM & {
				rumor: NonNullable<EventBBGM["rumor"]>;
			} => e.type === "rumor" && e.season === season && e.rumor !== undefined,
		)
		.sort((a, b) => a.eid - b.eid);
	const today = await eventDate();
	// Whether a story told then is still recent.
	const recent = (e: EventBBGM, i: number) =>
		today !== undefined && e.date !== undefined
			? daysBetween(e.date, today) < rumorSettings.quietDays
			: told.length - 1 - i < rumorSettings.quietStories;
	// A player again only once his talks go further than lately, never the
	// same story twice: a talk with the same team, or teams after him, far
	// apart or his team open to moving him, once a season each.
	const quiet = (pid: number, kind: Kind, tier: number, buyer?: number) =>
		told.some((e, i) => {
			if (!e.pids?.includes(pid)) {
				return false;
			}
			if (e.rumor.team !== undefined || e.rumor.tier > tier) {
				return recent(e, i);
			}
			return (
				e.rumor.tier === tier &&
				(recent(e, i) ||
					(tier === 1 ? e.rumor.kind === kind : e.rumor.buyer === buyer))
			);
		});
	// Teams in the news lately, so the same busy team isn't every story.
	const lately = new Map<number, number>();
	for (const [i, e] of told.entries()) {
		if (recent(e, i)) {
			for (const tid of e.tids ?? []) {
				lately.set(tid, (lately.get(tid) ?? 0) + 1);
			}
		}
	}
	const still = (p: Player, tid: number) => !traded.has(p.pid) && p.tid === tid;
	const penalty = (tids: readonly number[]) =>
		8 * tids.reduce((sum, tid) => sum + (lately.get(tid) ?? 0), 0);
	type Story = {
		tids: number[];
		pids: number[];
		weight: number;
		rumor: NonNullable<EventBBGM["rumor"]> & { kind: Kind | "team" };
		text: (seed: Seed) => string;
	};
	const stories: Story[] = [...now.values()]
		.filter((talk) => still(talk.p, talk.seller))
		.flatMap((talk): Story[] => {
			const kind = kindOf(talk);
			if (
				!kind ||
				(rank.get(talk.p.pid) ?? Infinity) >
					rumorSettings.topPlayers[kind.kind] ||
				quiet(talk.p.pid, kind.kind, kind.tier, kind.buyer)
			) {
				return [];
			}
			const tids =
				kind.buyer !== undefined
					? [kind.buyer, talk.seller]
					: kind.kind === "available"
						? [talk.seller, ...talk.bidders.slice(0, 2)]
						: [...talk.bidders.slice(0, 2), talk.seller];
			// Far apart is news only about teams not in the news lately (the
			// owner's league's busiest buyer had four in a row at the deadline).
			if (kind.kind === "apart" && tids.some((tid) => lately.has(tid))) {
				return [];
			}
			return [
				{
					tids,
					pids: [talk.p.pid],
					// A talk that got further is bigger news, and so is a better
					// player; a team already in the news less so.
					weight: talk.p.value + 6 * RANK_OF[kind.kind] - penalty(tids),
					rumor: {
						tier: kind.tier,
						kind: kind.kind,
						...(kind.buyer === undefined ? {} : { buyer: kind.buyer }),
					},
					text: (seed) => story(talk, kind.kind, seed),
				},
			];
		});
	// The busiest team after players this moment, named with its two best
	// targets, unless it was such a story lately.
	const after = new Map<number, Player[]>();
	for (const talk of now.values()) {
		for (const tid of talk.bidders) {
			after.set(tid, [...(after.get(tid) ?? []), talk.p]);
		}
	}
	const teamLately = told.some(
		(e, i) => e.rumor.team !== undefined && recent(e, i),
	);
	const busiest = teamLately
		? undefined
		: [...after]
				.filter(
					([tid, ps]) =>
						ps.length >= rumorSettings.busyTeam &&
						!told.some((e) => e.rumor.team === tid),
				)
				.sort((a, b) => b[1].length - a[1].length || a[0] - b[0])[0];
	if (busiest) {
		const [tid, ps] = busiest;
		// Its best targets the news would know, not told about lately.
		const targets = ps
			.filter(
				(p) =>
					p.tid >= 0 &&
					!traded.has(p.pid) &&
					(rank.get(p.pid) ?? Infinity) <= rumorSettings.busyTargets &&
					!told.some((e, i) => e.pids?.includes(p.pid) && recent(e, i)),
			)
			.sort((a, b) => b.value - a.value || a.pid - b.pid)
			.slice(0, 2);
		if (targets.length > 0) {
			stories.push({
				tids: [tid],
				pids: targets.map((p) => p.pid),
				weight: targets[0]!.value + 6 * RANK_OF.team - penalty([tid]),
				rumor: { tier: 1, kind: "team", team: tid },
				text: (seed) => teamStory(tid, ps.length, targets, seed),
			});
		}
	}
	stories.sort((a, b) => b.weight - a.weight || a.pids[0]! - b.pids[0]!);
	const toldOfKind = new Map<string, number>();
	for (const e of told) {
		const kind = e.rumor.kind ?? "";
		toldOfKind.set(kind, (toldOfKind.get(kind) ?? 0) + 1);
	}
	const teamsTold = new Set<number>();
	let n = 0;
	for (const { tids, pids, rumor, text } of stories) {
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
		const ofKind = toldOfKind.get(rumor.kind) ?? 0;
		toldOfKind.set(rumor.kind, ofKind + 1);
		await logEvent({
			type: "rumor",
			text: text({ kind: ofKind, all: told.length + n }),
			pids,
			tids,
			score:
				(rank.get(pids[0]!) ?? Infinity) <= rumorSettings.starRank ? 15 : 10,
			rumor,
			showNotification: false,
		});
		n += 1;
	}
};
