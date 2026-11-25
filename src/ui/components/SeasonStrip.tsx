import clsx from "clsx";
import { useLayoutEffect, useRef, useState } from "react";
import getWinner from "../../common/getWinner.ts";
import {
	dayNumber,
	formatGameDate,
	formatMonth,
} from "../../common/calendarDates.ts";
import { isAllStarGame } from "../../common/specialGames.ts";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { useSimTimes } from "../util/simTimes.ts";

// A TEAM'S SEASON AS ONE PICTURE, the way Basketball-Reference draws it: a
// slot for every date, grouped by month, a bar on each game day - up and
// green for a win, down and red for a loss, as tall as the margin - a dash on
// each day off, and a gray block for each game still to play. The playoffs are
// a group of their own at the end. Each bar is its box score.

type PlayedGame = {
	gid: number;
	day?: number;
	date?: string;
	playoffs?: boolean;
	overtimes?: number;
	numPeriods?: number;
	teams: [{ tid: number; pts: number }, { tid: number; pts: number }];
};

type ComingGame = {
	gid: number;
	day: number;
	date?: string;
	live?: true;
	teams: [{ tid: number }, { tid: number }];
};

type Entry = {
	gid: number;
	slot: number;
	group: string;
	result: "W" | "L" | "T" | undefined;
	margin: number;
	label: string;
	// When auto play sims a game still to come (see simTimes.ts).
	simTime?: string;
};

const STEP = 8;
const BAR = 6;
const HALF = 44;
const CAP = 40;
const NEXT = 7;
// Days between one month's group and the next.
const GAP = 3;
// About how wide a capital letter of a group's name is, in px (it's smaller on
// a phone).
const charWidth = (axisWidth: number) => (axisWidth < 540 ? 5.4 : 6.1);

export const SeasonStrip = ({
	games,
	upcoming,
	upcomingPlayoffs,
	tid,
	season,
}: {
	games: PlayedGame[];
	upcoming: ComingGame[];
	upcomingPlayoffs: boolean;
	tid: number;
	season: number;
}) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const [hover, setHover] = useState<number | undefined>();
	const simTimes = useSimTimes();

	// The month names under the groups need the strip's real width.
	const axisRef = useRef<HTMLDivElement>(null);
	const [axisWidth, setAxisWidth] = useState(0);
	useLayoutEffect(() => {
		const el = axisRef.current;
		if (!el) {
			return;
		}
		setAxisWidth(el.clientWidth);
		const observer = new ResizeObserver(() => {
			setAxisWidth(el.clientWidth);
		});
		observer.observe(el);
		return () => {
			observer.disconnect();
		};
	}, []);

	const abbrev = teamInfoCache[tid]?.abbrev ?? "";
	const oppAbbrev = (t: number) => teamInfoCache[t]?.abbrev ?? "";
	const mine = (game: { teams: [{ tid: number }, { tid: number }] }) =>
		!isAllStarGame(game.teams[0].tid, game.teams[1].tid) &&
		game.teams.some((t) => t.tid === tid);
	const slotOf = (game: { day?: number; date?: string }) =>
		game.date ? dayNumber(game.date) : (game.day ?? 0);
	const groupOf = (game: { date?: string }, playoffs: boolean) =>
		playoffs ? "playoffs" : game.date ? game.date.slice(0, 7) : "season";
	const when = (date: string | undefined) =>
		date ? `${formatGameDate(date, "monthDay")}, ` : "";

	const entries: Entry[] = [];

	// Records as of each game: the season's, and in the playoffs the series'.
	const record = { W: 0, L: 0, T: 0 };
	const series = new Map<number, { W: number; L: number; next?: number }>();
	let n = 0;
	const played = games
		.filter(mine)
		.slice()
		.sort((a, b) => (a.day ?? 0) - (b.day ?? 0) || a.gid - b.gid);
	for (const game of played) {
		const i = game.teams[0].tid === tid ? 0 : 1;
		const me = game.teams[i];
		const opp = game.teams[i === 0 ? 1 : 0];
		const winner = getWinner(game.teams);
		const result = winner === -1 ? "T" : winner === i ? "W" : "L";
		const ot = helpers.overtimeText(game.overtimes ?? 0, game.numPeriods);
		const score = `${me.pts}-${opp.pts}${ot ? ` ${ot}` : ""}`;
		const verb = result === "W" ? "beat" : result === "L" ? "lost to" : "tied";
		let head;
		let standing;
		if (game.playoffs) {
			const s = series.get(opp.tid) ?? { W: 0, L: 0 };
			if (result === "W" || result === "L") {
				s[result] += 1;
			}
			series.set(opp.tid, s);
			head = `Game ${s.W + s.L}`;
			standing = `${s.W}-${s.L}`;
		} else {
			n += 1;
			record[result] += 1;
			head = `${n}`;
			standing = `${record.W}-${record.L}${record.T > 0 ? `-${record.T}` : ""}`;
		}
		entries.push({
			gid: game.gid,
			slot: slotOf(game),
			group: groupOf(game, !!game.playoffs),
			result,
			margin: me.pts - opp.pts,
			label: `${head}. ${when(game.date)}${abbrev} (${standing}) ${verb} ${oppAbbrev(opp.tid)}, ${score}`,
		});
	}
	for (const game of upcoming.filter(mine)) {
		const home = game.teams[0].tid === tid;
		const opp = game.teams[home ? 1 : 0];
		let head;
		if (upcomingPlayoffs) {
			const s = series.get(opp.tid) ?? { W: 0, L: 0 };
			const next = (s.next ?? s.W + s.L) + 1;
			series.set(opp.tid, { ...s, next });
			head = `Game ${next}`;
		} else {
			n += 1;
			head = `${n}`;
		}
		entries.push({
			gid: game.gid,
			slot: slotOf(game),
			group: groupOf(game, upcomingPlayoffs),
			result: undefined,
			margin: 0,
			label: `${head}. ${when(game.date)}${abbrev} ${home ? "vs" : "at"} ${oppAbbrev(opp.tid)}${game.live ? ", live" : ""}`,
			simTime: game.live
				? undefined
				: (simTimes.date(game.date) ?? simTimes.day(season, game.day)),
		});
	}
	if (entries.length === 0) {
		return null;
	}

	// The groups in order, each from its first game day to its last.
	const groups: {
		key: string;
		label: string;
		from: number;
		to: number;
		x: number;
	}[] = [];
	for (const entry of entries) {
		const group = groups.find((g) => g.key === entry.group);
		if (group) {
			group.from = Math.min(group.from, entry.slot);
			group.to = Math.max(group.to, entry.slot);
		} else {
			groups.push({
				key: entry.group,
				label:
					entry.group === "playoffs"
						? "Playoffs"
						: entry.group === "season"
							? ""
							: formatMonth(`${entry.group}-01`, true),
				from: entry.slot,
				to: entry.slot,
				x: 0,
			});
		}
	}
	let x = 0;
	for (const group of groups) {
		group.x = x;
		x += (group.to - group.from + 1 + GAP) * STEP;
	}
	const width = x - GAP * STEP;
	const bySlot = new Map(
		entries.map((entry, k) => [`${entry.group}:${entry.slot}`, k]),
	);
	const xOf = (entry: Entry) => {
		const group = groups.find((g) => g.key === entry.group)!;
		return group.x + (entry.slot - group.from) * STEP;
	};
	const pct = (v: number) => `${(v / width) * 100}%`;

	const barOf = (entry: Entry) => {
		if (entry.result === undefined) {
			return { y: HALF - NEXT, h: NEXT * 2 };
		}
		const h = Math.max(
			2.5,
			(Math.min(CAP, Math.abs(entry.margin)) / CAP) * (HALF - 2),
		);
		return { y: entry.result === "W" ? HALF - h : HALF, h };
	};

	const tip = hover === undefined ? undefined : entries[hover];
	const tipX = tip ? xOf(tip) + STEP / 2 : 0;
	const tipBar = tip ? barOf(tip) : undefined;

	return (
		<section className="season-strip">
			<h2 className="season-strip-title">Game Results</h2>
			<div className="season-strip-plot">
				<svg
					className="season-strip-svg"
					viewBox={`0 0 ${width} ${HALF * 2}`}
					preserveAspectRatio="none"
				>
					{groups.flatMap((group) => {
						const out = [];
						for (let slot = group.from; slot <= group.to; slot++) {
							if (!bySlot.has(`${group.key}:${slot}`)) {
								out.push(
									<rect
										key={`${group.key}:${slot}`}
										className="season-strip-off"
										x={group.x + (slot - group.from) * STEP + 1}
										y={HALF - 0.6}
										width={STEP - 2}
										height={1.2}
									/>,
								);
							}
						}
						return out;
					})}
					{entries.map((entry, k) => {
						const { y, h } = barOf(entry);
						const rect = (
							<rect
								className={
									entry.result === "W"
										? "season-strip-win"
										: entry.result === "L"
											? "season-strip-loss"
											: entry.result === "T"
												? "season-strip-tie"
												: "season-strip-next"
								}
								x={xOf(entry) + (STEP - BAR) / 2}
								y={y}
								width={BAR}
								height={h}
							/>
						);
						return entry.result === undefined ? (
							<g
								key={entry.gid}
								onMouseEnter={() => setHover(k)}
								onMouseLeave={() => setHover(undefined)}
								aria-label={entry.label}
								role="img"
							>
								{rect}
							</g>
						) : (
							<a
								key={entry.gid}
								href={helpers.leagueUrl([
									"game_log",
									`${abbrev}_${tid}`,
									season,
									entry.gid,
								])}
								aria-label={entry.label}
								onMouseEnter={() => setHover(k)}
								onMouseLeave={() => setHover(undefined)}
							>
								{/* (A slot wide and the strip tall, so a thin bar
								    is easy to hit.) */}
								<rect
									className="season-strip-hit"
									x={xOf(entry)}
									y={0}
									width={STEP}
									height={HALF * 2}
								/>
								{rect}
							</a>
						);
					})}
				</svg>
				{tip && tipBar ? (
					<div
						className={clsx(
							"season-strip-tip",
							tipX / width < 0.15
								? "season-strip-tip-start"
								: tipX / width > 0.85
									? "season-strip-tip-end"
									: undefined,
						)}
						style={{
							left: pct(tipX),
							bottom: `calc(${(1 - tipBar.y / (HALF * 2)) * 100}% + 8px)`,
						}}
					>
						{tip.label}
						{tip.simTime ? (
							<>
								<br />
								{tip.simTime}
							</>
						) : null}
					</div>
				) : null}
			</div>
			<div className="season-strip-groups" ref={axisRef}>
				{groups.map((group, j) => {
					const w = (group.to - group.from + 1) * STEP;
					const px = (v: number) => (v / width) * axisWidth;
					const need = group.label.length * charWidth(axisWidth);
					// A name may run on into the gap, not into the next name. The
					// last one, if it's short of room, ends at the strip's end
					// instead, unless that runs it into the one before.
					let shift = 0;
					let fits = need <= px((groups[j + 1]?.x ?? width) - group.x) - 4;
					const prev = groups[j - 1];
					if (!fits && j === groups.length - 1 && prev) {
						shift = axisWidth - need - px(group.x);
						fits =
							px(group.x) + shift >=
							px(prev.x) + prev.label.length * charWidth(axisWidth) + 8;
					}
					return (
						<div
							key={group.key}
							className="season-strip-group"
							style={{ left: pct(group.x), width: pct(w) }}
						>
							{group.label && axisWidth > 0 && fits ? (
								<span style={shift !== 0 ? { left: shift } : undefined}>
									{group.label}
								</span>
							) : null}
						</div>
					);
				})}
			</div>
		</section>
	);
};
