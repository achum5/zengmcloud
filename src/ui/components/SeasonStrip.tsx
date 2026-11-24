import { useLayoutEffect, useRef, useState, type CSSProperties } from "react";
import getWinner from "../../common/getWinner.ts";
import { formatGameDate } from "../../common/calendarDates.ts";
import { isAllStarGame } from "../../common/specialGames.ts";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { contrast } from "../../common/stockCourts.ts";
import { useAllTeamColors } from "./TeamBanner.tsx";

// A TEAM'S SEASON AS ONE PICTURE: a bar a game, oldest first, up for a win
// and down for a loss, as tall as the margin, in the team's color. The
// longest winning run is shaded, the months are marked under it, and the
// playoffs follow a line. Each bar is its box score.

type StripGame = {
	gid: number;
	day?: number;
	date?: string;
	playoffs?: boolean;
	overtimes?: number;
	numPeriods?: number;
	teams: [{ tid: number; pts: number }, { tid: number; pts: number }];
};

const HEX = /^#[\da-f]{6}$/i;

// The team's first color that shows up on the page, light or dark.
const winColor = (colors: string[] | undefined, page: string) =>
	(colors ?? [])
		.filter((c) => HEX.test(c))
		.find((c) => contrast(c, page) >= 1.8) ??
	(page === "#ffffff" ? "#343a40" : "#ced4da");

const CAP = 30;
const STEP = 10;
const BAR = 7;
const HALF = 40;

export const SeasonStrip = ({
	games,
	tid,
	season,
}: {
	games: StripGame[];
	tid: number;
	season: number;
}) => {
	const colors = useAllTeamColors();
	const { teamInfoCache } = useLocal(["teamInfoCache"]);

	// The labels under the bars are spaced by the strip's real width.
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
	const list = games
		.filter((game) => !isAllStarGame(game.teams[0].tid, game.teams[1].tid))
		.filter((game) => game.teams.some((t) => t.tid === tid))
		.slice()
		.sort((a, b) => (a.day ?? 0) - (b.day ?? 0) || a.gid - b.gid);
	if (list.length === 0) {
		return null;
	}

	const rows = list.map((game) => {
		const i = game.teams[0].tid === tid ? 0 : 1;
		const me = game.teams[i];
		const opp = game.teams[i === 0 ? 1 : 0];
		const winner = getWinner(game.teams);
		return {
			game,
			home: i === 0,
			opp,
			margin: me.pts - opp.pts,
			result: winner === -1 ? "T" : winner === i ? "W" : "L",
		};
	});

	// The longest winning run.
	let best = { from: -1, len: 0 };
	let run = 0;
	for (const [k, row] of rows.entries()) {
		run = row.result === "W" ? run + 1 : 0;
		if (run > best.len) {
			best = { from: k - run + 1, len: run };
		}
	}

	const n = rows.length;
	const width = n * STEP;
	const playoffStart = rows.findIndex((row) => row.game.playoffs);
	const regular = rows.filter((row) => !row.game.playoffs);
	const playoffs = rows.filter((row) => row.game.playoffs);
	const record = (r: typeof rows) =>
		`${r.filter((row) => row.result === "W").length}-${r.filter((row) => row.result === "L").length}`;

	// A label at the first game of each month.
	const months: { k: number; label: string }[] = [];
	for (const [k, row] of rows.entries()) {
		if (row.game.playoffs || !row.game.date) {
			continue;
		}
		const label = formatGameDate(row.game.date, "monthDay").slice(0, 3);
		if (months.at(-1)?.label !== label) {
			months.push({ k, label });
		}
	}

	// The playoffs are named beside their line (before it, when there is no
	// room after it), and a month whose name would run into the next name is
	// left unnamed.
	const LABEL = 30;
	const PLAYOFFS_LABEL = 64;
	const x = (k: number) => (k / n) * axisWidth;
	const playoffsFlip = x(playoffStart) + 4 + PLAYOFFS_LABEL > axisWidth;
	const playoffsX =
		playoffStart <= 0
			? Infinity
			: playoffsFlip
				? x(playoffStart) - 4 - PLAYOFFS_LABEL
				: x(playoffStart) + 4;
	const monthLabels = months.filter((m, j) => {
		if (axisWidth === 0) {
			return (months[j + 1]?.k ?? Infinity) - m.k >= 4;
		}
		const next = months[j + 1] ? x(months[j + 1]!.k) : playoffsX;
		return Math.min(next, playoffsX) - x(m.k) >= LABEL;
	});

	const abbrev = teamInfoCache[tid]?.abbrev ?? "";
	const pct = (k: number) => `${((k * STEP) / width) * 100}%`;

	return (
		<section
			className="season-strip"
			style={
				{
					"--ss-win-light": winColor(colors?.[tid], "#ffffff"),
					"--ss-win-dark": winColor(colors?.[tid], "#212529"),
				} as CSSProperties
			}
		>
			<div className="season-strip-head">
				<h2 className="season-strip-title">Game by game</h2>
				<div className="season-strip-records">
					<span>
						<b>{record(regular)}</b> regular season
					</span>
					{playoffs.length > 0 ? (
						<span>
							<b>{record(playoffs)}</b> playoffs
						</span>
					) : null}
				</div>
			</div>
			<div className="season-strip-plot">
				{best.len >= 3 ? (
					<div
						className="season-strip-run"
						style={{
							left: pct(best.from),
							width: pct(best.len),
						}}
					>
						<span>W{best.len}</span>
					</div>
				) : null}
				{playoffStart > 0 ? (
					<div
						className="season-strip-divider"
						style={{ left: pct(playoffStart) }}
					/>
				) : null}
				<svg
					className="season-strip-svg"
					viewBox={`0 0 ${width} ${HALF * 2}`}
					preserveAspectRatio="none"
					role="img"
					aria-label={`${abbrev} ${record(regular)}`}
				>
					<line
						className="season-strip-base"
						x1={0}
						x2={width}
						y1={HALF}
						y2={HALF}
					/>
					{rows.map((row, k) => {
						const h = Math.max(
							2.5,
							(Math.min(CAP, Math.abs(row.margin)) / CAP) * (HALF - 2),
						);
						const x = k * STEP + (STEP - BAR) / 2;
						const y = row.result === "W" ? HALF - h : HALF;
						const opp = teamInfoCache[row.opp.tid];
						const ot = helpers.overtimeText(
							row.game.overtimes ?? 0,
							row.game.numPeriods,
						);
						const when = row.game.date
							? formatGameDate(row.game.date)
							: `Game ${k + 1}`;
						return (
							<a
								key={row.game.gid}
								href={helpers.leagueUrl([
									"game_log",
									`${abbrev}_${tid}`,
									season,
									row.game.gid,
								])}
							>
								<title>
									{`${when} · ${row.home ? "vs" : "@"} ${opp?.abbrev ?? ""} · ${row.result} ${Math.max(row.game.teams[0].pts, row.game.teams[1].pts)}-${Math.min(row.game.teams[0].pts, row.game.teams[1].pts)}${ot ? ` ${ot}` : ""}`}
								</title>
								<rect
									className={
										row.result === "W"
											? "season-strip-win"
											: "season-strip-loss"
									}
									x={x}
									y={y}
									width={BAR}
									height={h}
								/>
							</a>
						);
					})}
				</svg>
			</div>
			<div className="season-strip-months" ref={axisRef}>
				{monthLabels.map((m) => (
					<span key={m.k} style={{ left: pct(m.k) }}>
						{m.label}
					</span>
				))}
				{playoffStart > 0 && axisWidth > 0 ? (
					<span
						className="season-strip-playoffs"
						style={{
							left: playoffsX,
							width: PLAYOFFS_LABEL,
							textAlign: playoffsFlip ? "right" : "left",
						}}
					>
						Playoffs
					</span>
				) : null}
			</div>
		</section>
	);
};
