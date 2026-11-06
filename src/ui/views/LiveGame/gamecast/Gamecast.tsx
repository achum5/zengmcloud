// THE LIVE GAME AS A GAMECAST: the court on top with the score by quarter and
// the ten men on the floor beside it, the latest play right under the
// picture, and tabs under that - Gamecast (a bit of everything, scrolling
// down), the box score, every play, the team stats and the game flow.
// (Basketball, and only on a device that has turned the layout on - see
// local.gamecastLayout.)
//
// Laid out the way the rest of the game is: a heading and what it heads,
// tables and dividers - no boxes round everything, no colored stripes or
// pill badges to say what the words already say.

import clsx from "clsx";
import {
	createContext,
	Fragment,
	memo,
	useContext,
	useMemo,
	useRef,
	useState,
	type ReactNode,
} from "react";
import { helpers } from "../../../util/helpers.ts";
import { useLocal } from "../../../util/local.ts";
import { usePlayerFace } from "../../../util/playerFaces.ts";
import { PlayerPicture } from "../../../components/PlayerPicture.tsx";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import { SkillsBlock } from "../../../components/SkillsBlock.tsx";
import { RatingsStatsPopover } from "../../../components/RatingsStatsPopover/index.tsx";
import { sideColors } from "../../../components/GameCharts.basketball.tsx";
import { TEAM_NUMS_DISPLAY_ORDER } from "../../../util/boxScoreDisplayOrder.ts";
import {
	feedRows,
	highlightOf,
	periodLabel,
	type FeedEntry,
	type FeedRow,
} from "./playFeed.ts";
import type { GamecastInfo } from "../../../../worker/views/liveGameInfo.ts";
import { FlowCharts } from "./FlowCharts.tsx";
import { playSentence } from "./sentence.ts";
import { ShotChart } from "./ShotChart.tsx";
import { HighlightClip, type ClipGame } from "./HighlightClip.tsx";
import type { ShotSpot } from "./shots.ts";

type Tab = "gamecast" | "box" | "plays" | "stats" | "flow";

const TABS: [Tab, string][] = [
	["gamecast", "Gamecast"],
	["box", "Box Score"],
	["plays", "Play-by-Play"],
	["stats", "Team Stats"],
	["flow", "Game Flow"],
];

const shortName = (name: string | undefined) => {
	if (!name) {
		return "";
	}
	const parts = name.split(" ");
	return parts.length > 1
		? `${parts[0]![0]}. ${parts.slice(1).join(" ")}`
		: name;
};

const playerUrl = (pid: number) => helpers.leagueUrl(["player", pid]);

export const Logo = ({ t, size = 24 }: { t: any; size?: number }) => (
	<TeamLogoInline
		alt={t.abbrev}
		className="flex-shrink-0"
		imgURL={t.imgURL}
		imgURLSmall={t.imgURLSmall}
		includePlaceholderIfNoLogo
		size={size}
	/>
);

// His face in a circle, head and shoulders, as a broadcast's graphics show a
// player (loaded on demand and cached - see usePlayerFace). A drawn face is
// 400 x 600 with his head from about 100 down to 505 (his hair from 55) and
// his shirt from 300: the circle takes a window FACE_WINDOW across from
// FACE_TOP down, centered - all of his head, a little of his shoulders. A
// photo fills the circle from its top.
const FACE_TOP = 32;
const FACE_WINDOW = 548;
export const Headshot = ({
	pid,
	season,
	size = 36,
}: {
	pid: number;
	season: number;
	size?: number;
}) => {
	const { lid } = useLocal(["lid"]);
	const face = usePlayerFace(pid, season, lid);
	const k = size / FACE_WINDOW;
	const photo = !!face?.imgURL;
	return (
		<div
			className={clsx("gamecast-headshot flex-shrink-0", {
				"gamecast-headshot-photo": photo,
			})}
			style={{ width: size, height: size }}
		>
			{face && (face.face || face.imgURL) ? (
				<div
					style={
						photo
							? { width: size, height: size }
							: {
									width: 400 * k,
									height: 600 * k,
									marginTop: -FACE_TOP * k,
									marginLeft: ((FACE_WINDOW - 400) / 2) * k,
								}
					}
				>
					<PlayerPicture
						face={face.face}
						imgURL={face.imgURL}
						colors={face.colors}
						jersey={face.jersey}
						lazy
					/>
				</div>
			) : null}
		</div>
	);
};

// A heading and what it heads.
export const Section = ({
	title,
	action,
	children,
	className,
}: {
	title?: string;
	action?: ReactNode;
	children: ReactNode;
	className?: string;
}) => (
	<section className={clsx("gamecast-section", className)}>
		{title !== undefined || action ? (
			<div className="gamecast-section-head">
				{title !== undefined ? <h2>{title}</h2> : null}
				{action ? <div className="ms-auto">{action}</div> : null}
			</div>
		) : null}
		{children}
	</section>
);

export const LinkButton = ({
	children,
	onClick,
}: {
	children: ReactNode;
	onClick: () => void;
}) => (
	<button type="button" className="btn btn-link p-0" onClick={onClick}>
		{children}
	</button>
);

// Bootstrap's button group, as the game's own switches are (see GameCharts).
export const Switch = <T extends string | number>({
	options,
	value,
	onChange,
}: {
	options: [T, string][];
	value: T;
	onChange: (value: T) => void;
}) => (
	<div className="btn-group btn-group-sm flex-wrap" role="group">
		{options.map(([option, label]) => (
			<button
				key={option}
				type="button"
				className={`btn ${option === value ? "btn-secondary" : "btn-light-bordered"}`}
				onClick={() => onChange(option)}
			>
				{label}
			</button>
		))}
	</div>
);

// --- Plays -------------------------------------------------------------------

const textOf = (texts: unknown[]) =>
	texts.map((text, i) => {
		const prev = texts[i - 1];
		const sep =
			i === 0
				? null
				: typeof prev === "string" && !/[!).?]$/.test(prev.trim())
					? ". "
					: " ";
		return (
			<Fragment key={i}>
				{sep}
				{text as ReactNode}
			</Fragment>
		);
	});

const Score = ({ row }: { row: FeedRow }) => {
	if (!row.pts) {
		return null;
	}
	return (
		<span className="gamecast-play-score text-nowrap">
			{TEAM_NUMS_DISPLAY_ORDER.map((t, i) => (
				<Fragment key={t}>
					{i > 0 ? <span className="gamecast-score-gap" /> : null}
					<span
						className={
							row.scored !== undefined && row.t === t
								? "gamecast-scorer"
								: undefined
						}
					>
						{row.pts![t]}
					</span>
				</Fragment>
			))}
		</span>
	);
};

// Whose play it is: his face, his team's logo on its corner - or, for a
// play that is nobody's (a timeout), the team's logo alone.
export const PlayPicture = ({
	pid,
	team,
	season,
	size,
}: {
	pid: number | undefined;
	team: any;
	season: number;
	size: number;
}) => {
	if (pid === undefined) {
		return team ? <Logo t={team} size={Math.round(size * 0.8)} /> : null;
	}
	return (
		<div className="gamecast-play-face" style={{ width: size, height: size }}>
			<Headshot pid={pid} season={season} size={size} />
			{team ? (
				<div className="gamecast-play-badge">
					<Logo t={team} size={Math.round(size * 0.42)} />
				</div>
			) : null}
		</div>
	);
};

// How the plays listed are said (see sentence.ts) and what tapping one
// does (shows it again on the court, or opens the replay at it) - or the
// play itself, to be opened under its row (`clip`, see HighlightClip) - set
// by the page listing them.
export const FeedContext = createContext<{
	say?: (row: FeedRow) => string | undefined;
	open?: (row: FeedRow) => void;
	clip?: (row: FeedRow) => ReactNode;
}>({});

// Lines with no play of their own to show.
const UNWATCHED = new Set([
	"sub",
	"timeout",
	"injury",
	"foulOut",
	"elamActive",
	"shootoutStart",
	"shootoutTie",
	"shootoutTeam",
	"gameOver",
	"period",
	"overtime",
	"endOfPeriod",
]);
const watchable = (row: FeedRow) =>
	row.cursor !== undefined &&
	!row.pending &&
	!row.textOnly &&
	!UNWATCHED.has(row.type ?? "");

// Down, or turned up while its play is open under it.
const Chevron = ({ open }: { open: boolean }) => (
	<span
		className={clsx("glyphicon glyphicon-menu-down gamecast-chevron", {
			open,
		})}
	/>
);

export const PlayRow = memo(
	({
		row,
		teams,
		season,
		numPeriods,
		latest,
		opened,
		onOpen,
	}: {
		row: FeedRow;
		teams: [any, any];
		season: number;
		numPeriods: number;
		// The one under the court: bigger, and two lines of text at most.
		latest?: boolean;
		// In a list, its play open under it (see PlayList).
		opened?: boolean;
		onOpen?: (key: number | undefined) => void;
	}) => {
		const { say, open, clip } = useContext(FeedContext);
		if (row.textOnly) {
			return <div className="gamecast-divider">{textOf(row.texts)}</div>;
		}
		const team = row.t !== undefined ? teams[row.t] : undefined;
		const sentence = say?.(row);
		const watch = clip !== undefined && onOpen !== undefined && watchable(row);
		const opens =
			watch || (open !== undefined && row.cursor !== undefined && !row.pending);
		const act = () => {
			if (watch) {
				onOpen(opened ? undefined : row.key);
			} else {
				open?.(row);
			}
		};
		const when = [
			row.time,
			periodLabel(row.period, numPeriods, helpers.ordinal),
		]
			.filter(Boolean)
			.join(" - ");
		const head = (
			<div
				className={clsx("gamecast-play", {
					"gamecast-play-latest": latest,
					"gamecast-play-opens": opens,
				})}
				{...(opens
					? {
							role: "button",
							tabIndex: 0,
							title: watch && opened ? "Close" : "Watch it",
							...(watch ? { "aria-expanded": !!opened } : {}),
							onClick: act,
							onKeyDown: (e: { key: string }) => {
								if (e.key === "Enter") {
									act();
								}
							},
						}
					: {})}
			>
				<div className="gamecast-play-picture">
					<PlayPicture
						pid={row.pid}
						team={team}
						season={season}
						size={latest ? 44 : 38}
					/>
				</div>
				<div className="flex-grow-1" style={{ minWidth: 0 }}>
					<div className="d-flex align-items-baseline">
						<div className="gamecast-play-title">
							{row.title || "Play"}
							{when ? <span className="gamecast-play-when">{when}</span> : null}
						</div>
						<div className="ms-auto ps-3">
							<Score row={row} />
						</div>
					</div>
					<div
						className={clsx("gamecast-play-text", {
							"gamecast-clamp": latest,
						})}
					>
						{sentence ?? textOf(row.texts)}
					</div>
				</div>
				{watch ? <Chevron open={!!opened} /> : null}
			</div>
		);
		if (!watch) {
			return head;
		}
		return (
			<div className="gamecast-unit">
				{head}
				{opened ? <div className="gamecast-clip">{clip(row)}</div> : null}
			</div>
		);
	},
	(a, b) =>
		a.row.key === b.row.key &&
		a.row.title === b.row.title &&
		a.row.texts.length === b.row.texts.length &&
		a.latest === b.latest &&
		a.opened === b.opened &&
		a.onOpen === b.onOpen,
);

export const PlayList = ({
	rows,
	teams,
	season,
	numPeriods,
}: {
	rows: FeedRow[];
	teams: [any, any];
	season: number;
	numPeriods: number;
}) => {
	// (One play open at a time.)
	const [opened, setOpened] = useState<number | undefined>();
	return (
		<div className="gamecast-plays">
			{rows.map((row) => (
				<PlayRow
					key={row.key}
					row={row}
					teams={teams}
					season={season}
					numPeriods={numPeriods}
					opened={opened === row.key}
					onOpen={setOpened}
				/>
			))}
			{rows.length === 0 ? (
				<div className="text-body-secondary py-3">No plays yet.</div>
			) : null}
		</div>
	);
};

export const AllPlays = ({
	rows,
	teams,
	season,
	numPeriods,
}: {
	rows: FeedRow[];
	teams: [any, any];
	season: number;
	numPeriods: number;
}) => {
	const periods = useMemo(() => {
		const set = new Set<number>();
		for (const row of rows) {
			if (row.period !== undefined && row.period > 0) {
				set.add(row.period);
			}
		}
		return [...set].sort((a, b) => a - b);
	}, [rows]);
	const [period, setPeriod] = useState<number | "all">("all");
	const [scoring, setScoring] = useState<"all" | "scoring">("all");
	const shown = rows.filter(
		(row) =>
			(period === "all" || row.period === period) &&
			(scoring === "all" || row.scored !== undefined),
	);
	return (
		<Section title="Play-by-Play">
			<div className="d-flex flex-wrap gap-2 mb-2">
				{periods.length > 1 ? (
					<Switch
						options={[
							["all", "All"] as [number | "all", string],
							...periods.map(
								(p) =>
									[p, periodLabel(p, numPeriods, helpers.ordinal) ?? ""] as [
										number | "all",
										string,
									],
							),
						]}
						value={period}
						onChange={setPeriod}
					/>
				) : null}
				<Switch
					options={[
						["all", "All plays"],
						["scoring", "Scoring plays"],
					]}
					value={scoring}
					onChange={setScoring}
				/>
			</div>
			<PlayList
				rows={shown}
				teams={teams}
				season={season}
				numPeriods={numPeriods}
			/>
		</Section>
	);
};

// How big a highlight is, for the few a finished game shows (see Highlights).
const HIGHLIGHT_WEIGHT: Record<string, number> = {
	"Poster dunk": 6,
	"Dunk and the foul": 5,
	"And-one": 4,
	Dunk: 3,
	Block: 2,
	Three: 2,
};

// The game's dunks, threes and blocks so far, newest first - each one, opened,
// played right there under its row (see FeedContext's clip). A game over
// (`best`): its biggest few, in the order they happened, and all of them in
// that order.
export const Highlights = ({
	rows,
	teams,
	season,
	numPeriods,
	best,
}: {
	rows: FeedRow[];
	teams: [any, any];
	season: number;
	numPeriods: number;
	best?: boolean;
}) => {
	const [all, setAll] = useState(false);
	const [open, setOpen] = useState<number | undefined>();
	const { say, clip } = useContext(FeedContext);
	const newest = rows.filter((row) => highlightOf(row) !== undefined);
	if (newest.length === 0) {
		return null;
	}
	const SHOWN = 4;
	const highlights = best ? [...newest].reverse() : newest;
	let shown = highlights.slice(0, SHOWN);
	if (best && !all) {
		// (Late in a close one counts for more.)
		const late = (row: FeedRow) =>
			(row.period ?? 0) >= numPeriods &&
			row.pts !== undefined &&
			Math.abs(row.pts[0] - row.pts[1]) <= 6
				? 1
				: 0;
		const weight = (row: FeedRow) =>
			(HIGHLIGHT_WEIGHT[highlightOf(row)!] ?? 1) + late(row);
		const top = new Set(
			[...highlights]
				.sort((a, b) => weight(b) - weight(a))
				.slice(0, SHOWN)
				.map((row) => row.key),
		);
		shown = highlights.filter((row) => top.has(row.key));
	} else if (all) {
		shown = highlights;
	}
	return (
		<Section
			title="Highlights"
			action={
				highlights.length > SHOWN ? (
					<LinkButton onClick={() => setAll((a) => !a)}>
						{all ? "Fewer" : `All ${highlights.length}`}
					</LinkButton>
				) : null
			}
		>
			<div className="gamecast-plays">
				{shown.map((row) => {
					const team = row.t !== undefined ? teams[row.t] : undefined;
					const when = [
						row.time,
						periodLabel(row.period, numPeriods, helpers.ordinal),
					]
						.filter(Boolean)
						.join(" - ");
					const opens = clip !== undefined && watchable(row);
					const isOpen = opens && open === row.key;
					const toggle = () => setOpen(isOpen ? undefined : row.key);
					return (
						<div key={row.key} className="gamecast-unit">
							<div
								className={clsx("gamecast-play align-items-center", {
									"gamecast-play-opens": opens,
								})}
								{...(opens
									? {
											role: "button",
											tabIndex: 0,
											"aria-expanded": isOpen,
											title: isOpen ? "Close" : "Watch it",
											onClick: toggle,
											onKeyDown: (e: { key: string }) => {
												if (e.key === "Enter") {
													toggle();
												}
											},
										}
									: {})}
							>
								<div className="gamecast-play-picture">
									<PlayPicture
										pid={row.pid}
										team={team}
										season={season}
										size={38}
									/>
								</div>
								<div className="flex-grow-1" style={{ minWidth: 0 }}>
									<div className="gamecast-play-title">
										{highlightOf(row)}
										{when ? (
											<span className="gamecast-play-when">{when}</span>
										) : null}
									</div>
									<div className="gamecast-play-text gamecast-clamp">
										{say?.(row) ?? textOf(row.texts)}
									</div>
								</div>
								{opens ? <Chevron open={isOpen} /> : null}
							</div>
							{isOpen ? <div className="gamecast-clip">{clip(row)}</div> : null}
						</div>
					);
				})}
			</div>
		</Section>
	);
};

// --- Score and players -------------------------------------------------------

export const Linescore = ({ boxScore }: { boxScore: any }) => {
	const regulation = boxScore.numPeriods ?? 4;
	const numPeriods = Math.max(
		boxScore.teams[0].ptsQtrs?.length ?? 0,
		boxScore.teams[1].ptsQtrs?.length ?? 0,
		regulation,
	);
	const labels = Array.from({ length: numPeriods }, (_, i) =>
		i < regulation
			? String(i + 1)
			: i - regulation === 0
				? "OT"
				: `${i - regulation + 1}OT`,
	);
	const current = (boxScore.teams[0].ptsQtrs?.length ?? 0) - 1;
	return (
		<table className="table table-sm gamecast-table gamecast-linescore">
			<thead>
				<tr>
					<th />
					{labels.map((label, i) => (
						<th key={i} className={clsx({ current: i === current })}>
							{label}
						</th>
					))}
					<th className="total">T</th>
				</tr>
			</thead>
			<tbody>
				{TEAM_NUMS_DISPLAY_ORDER.map((ti) => {
					const t = boxScore.teams[ti];
					return (
						<tr key={ti}>
							<th>
								<div className="d-flex align-items-center gap-2">
									<Logo t={t} size={24} />
									{t.abbrev}
								</div>
							</th>
							{labels.map((_, i) => (
								<td key={i} className={clsx({ current: i === current })}>
									{t.ptsQtrs?.[i] ?? "-"}
								</td>
							))}
							<td className="total">{t.pts}</td>
						</tr>
					);
				})}
			</tbody>
		</table>
	);
};

const onFloor = (t: any): any[] => {
	const players: any[] = t.players ?? [];
	const on = players.filter((p) => p.inGame);
	// (Before the tip: the starters, who are listed first.)
	return on.length > 0 ? on : players.slice(0, 5);
};

export const OnCourt = ({ boxScore }: { boxScore: any }) => (
	<div className="table-responsive">
		<table className="table table-sm table-striped gamecast-table gamecast-oncourt">
			{TEAM_NUMS_DISPLAY_ORDER.map((ti) => {
				const t = boxScore.teams[ti];
				return (
					<tbody key={ti}>
						<tr className="gamecast-team-head">
							<th>
								<div className="d-flex align-items-center gap-2">
									<Logo t={t} size={18} />
									{t.abbrev}
								</div>
							</th>
							<th title="Game score">GmSc</th>
							<th title="Minutes played">MP</th>
							<th title="Points">PTS</th>
							<th title="Field goals">FG</th>
							<th title="Rebounds">REB</th>
							<th title="Assists">AST</th>
							<th title="Personal fouls">PF</th>
						</tr>
						{onFloor(t).map((p) => (
							<tr key={p.pid}>
								<td>
									<div className="d-flex align-items-center gap-2">
										<Headshot pid={p.pid} season={boxScore.season} size={22} />
										<div
											style={{ minWidth: 0 }}
											className="d-flex align-items-center"
										>
											<span className="text-truncate">
												<a href={playerUrl(p.pid)}>{shortName(p.name)}</a>{" "}
												<span className="text-body-secondary">{p.pos}</span>
											</span>
											<span className="flex-shrink-0 text-nowrap">
												<SkillsBlock skills={p.skills} />
												<RatingsStatsPopover
													pid={p.pid}
													season={boxScore.season}
												/>
											</span>
										</div>
									</div>
								</td>
								<td>{helpers.gameScore(p).toFixed(1)}</td>
								<td>{helpers.formatNumber(p.min, "minutes")}</td>
								<td className="fw-bold">{p.pts}</td>
								<td className="text-nowrap">
									{p.fg}-{p.fga}
								</td>
								<td>{p.orb + p.drb}</td>
								<td>{p.ast}</td>
								<td className={clsx({ "text-danger": p.pf >= 5 })}>{p.pf}</td>
							</tr>
						))}
					</tbody>
				);
			})}
		</table>
	</div>
);

const LEADER_STATS = [
	{
		label: "Points",
		value: (p: any) => p.pts,
		line: (p: any) => `${p.fg}/${p.fga} FG, ${p.ft}/${p.fta} FT`,
	},
	{
		label: "Rebounds",
		value: (p: any) => p.orb + p.drb,
		line: (p: any) => `${p.drb} DREB, ${p.orb} OREB`,
	},
	{
		label: "Assists",
		value: (p: any) => p.ast,
		line: (p: any) => `${p.tov} TO, ${Math.round(p.min)} MIN`,
	},
];

export const TeamsLine = ({ boxScore }: { boxScore: any }) => (
	<div className="gamecast-teams-line">
		{TEAM_NUMS_DISPLAY_ORDER.map((ti, i) => (
			<div
				key={ti}
				className={clsx("d-flex align-items-center gap-2", {
					"flex-row-reverse": i === 1,
				})}
			>
				<Logo t={boxScore.teams[ti]} size={24} />
				<span className="fw-bold">{boxScore.teams[ti].abbrev}</span>
			</div>
		))}
	</div>
);

export const Leaders = ({ boxScore }: { boxScore: any }) => (
	<div className="gl">
		<TeamsLine boxScore={boxScore} />
		{LEADER_STATS.map((stat) => {
			const best = TEAM_NUMS_DISPLAY_ORDER.map((ti) => {
				let top: any;
				for (const p of boxScore.teams[ti].players ?? []) {
					if (stat.value(p) > 0 && (!top || stat.value(p) > stat.value(top))) {
						top = p;
					}
				}
				return top;
			});
			return (
				<div key={stat.label} className="gl-row">
					<div className="gl-line">
						{best.map((p, i) => (
							<Fragment key={i}>
								{i === 1 ? <div className="gl-label">{stat.label}</div> : null}
								<div className={clsx("gl-side", { "gl-right": i === 1 })}>
									{p ? (
										<>
											<Headshot
												pid={p.pid}
												season={boxScore.season}
												size={40}
											/>
											<span className="gl-value">{stat.value(p)}</span>
										</>
									) : (
										<span className="text-body-secondary">-</span>
									)}
								</div>
							</Fragment>
						))}
					</div>
					<div className="gl-line">
						{best.map((p, i) => (
							<div key={i} className={clsx("gl-who", { "text-end": i === 1 })}>
								{p ? (
									<>
										<div className="text-truncate">
											<a href={playerUrl(p.pid)}>{shortName(p.name)}</a>
											{p.jerseyNumber ? (
												<span className="text-body-secondary">
													{" "}
													#{p.jerseyNumber}
												</span>
											) : null}
										</div>
										<div className="gl-stat">
											{/* (Each part kept whole, wrapping between them.) */}
											{stat
												.line(p)
												.split(", ")
												.map((part, j) => (
													<Fragment key={j}>
														{j > 0 ? ", " : null}
														<span className="text-nowrap">{part}</span>
													</Fragment>
												))}
										</div>
									</>
								) : null}
							</div>
						))}
					</div>
				</div>
			);
		})}
	</div>
);

// --- Team stats ----------------------------------------------------------------

// Leads over the game so far, from the live score line (see LiveFlow): the
// biggest each team has had, the share of the time it was ahead, and how
// often the lead changed hands. Box score order.
export const leadsOf = (
	flow: { scores: [number, number, number][]; now: number } | undefined,
) => {
	const largest: [number, number] = [0, 0];
	const ahead: [number, number] = [0, 0];
	let changes = 0;
	if (!flow) {
		return { largest, share: [0, 0] as [number, number], changes };
	}
	let leader: 0 | 1 | undefined;
	const scores = flow.scores;
	for (let i = 0; i < scores.length; i++) {
		const [time, a, b] = scores[i]!;
		const next = i + 1 < scores.length ? scores[i + 1]![0] : flow.now;
		const margin = a - b;
		if (margin > largest[0]) {
			largest[0] = margin;
		}
		if (-margin > largest[1]) {
			largest[1] = -margin;
		}
		const now = margin > 0 ? 0 : margin < 0 ? 1 : undefined;
		if (now !== undefined) {
			ahead[now] += Math.max(0, next - time);
			if (leader !== undefined && leader !== now) {
				changes += 1;
			}
			leader = now;
		}
	}
	const total = Math.max(1, flow.now);
	return {
		largest,
		share: [ahead[0] / total, ahead[1] / total] as [number, number],
		changes,
	};
};

const pct = (made: number, att: number) =>
	att > 0 ? `${Math.round((100 * made) / att)}%` : "-";

type StatRow = {
	label: string;
	// Box score order.
	values: [number, number];
	shown: [ReactNode, ReactNode];
	sub?: [ReactNode, ReactNode];
	// Fewer is better (turnovers, fouls).
	lowWins?: boolean;
};

const statRows = (boxScore: any, full: boolean): StatRow[] => {
	const [t0, t1] = boxScore.teams;
	const both = (f: (t: any) => number): [number, number] => [f(t0), f(t1)];
	const count = (
		label: string,
		f: (t: any) => number,
		lowWins?: boolean,
	): StatRow => {
		const values = both(f);
		return { label, values, shown: values, lowWins };
	};
	const shooting = (label: string, m: string, a: string): StatRow => ({
		label,
		values: both((t) => (t[a] > 0 ? t[m] / t[a] : 0)),
		shown: [pct(t0[m], t0[a]), pct(t1[m], t1[a])],
		sub: [`${t0[m]}-${t0[a]}`, `${t1[m]}-${t1[a]}`],
	});
	// (Live, the score line so far; a finished game's, all of it.)
	const leads = leadsOf(
		boxScore.liveFlow ??
			(boxScore.charts
				? { scores: boxScore.charts.scores, now: boxScore.charts.total }
				: undefined),
	);
	return [
		shooting("Field Goal %", "fg", "fga"),
		shooting("Three Point %", "tp", "tpa"),
		shooting("Free Throw %", "ft", "fta"),
		count("Rebounds", (t) => t.orb + t.drb),
		...(full
			? [
					count("Offensive Rebounds", (t) => t.orb),
					count("Defensive Rebounds", (t) => t.drb),
					count("Assists", (t) => t.ast),
					count("Steals", (t) => t.stl),
					count("Blocks", (t) => t.blk),
				]
			: []),
		count("Turnovers", (t) => t.tov, true),
		...(full ? [count("Fouls", (t) => t.pf, true)] : []),
		{
			label: "Percent Led",
			values: leads.share,
			shown: [
				`${Math.round(100 * leads.share[0])}%`,
				`${Math.round(100 * leads.share[1])}%`,
			],
		},
		{ label: "Largest Lead", values: leads.largest, shown: leads.largest },
	];
};

export const TeamStatBars = ({
	boxScore,
	colors,
	full,
}: {
	boxScore: any;
	colors: [string, string];
	full?: boolean;
}) => {
	const rows = statRows(boxScore, !!full);
	const [l, r] = TEAM_NUMS_DISPLAY_ORDER;
	return (
		<div>
			<TeamsLine boxScore={boxScore} />
			{rows.map((row) => {
				const max = Math.max(row.values[0], row.values[1]);
				// Whose number is better: that bar in the team's color, the other
				// greyed back.
				const better = (ti: 0 | 1) => {
					const mine = row.values[ti];
					const theirs = row.values[ti === 0 ? 1 : 0];
					return row.lowWins ? mine <= theirs : mine >= theirs;
				};
				return (
					<div key={row.label} className="gamecast-bar-row">
						<div className="gamecast-bar-values">
							<div>
								<span className="gamecast-bar-value">{row.shown[l]}</span>
								{row.sub ? (
									<span className="text-body-secondary ms-1">
										({row.sub[l]})
									</span>
								) : null}
							</div>
							<div className="gamecast-bar-label">{row.label}</div>
							<div className="text-end">
								{row.sub ? (
									<span className="text-body-secondary me-1">
										({row.sub[r]})
									</span>
								) : null}
								<span className="gamecast-bar-value">{row.shown[r]}</span>
							</div>
						</div>
						<div className="gamecast-bar-tracks">
							{TEAM_NUMS_DISPLAY_ORDER.map((ti, i) => (
								<div
									key={ti}
									className={clsx("gamecast-bar", { flip: i === 0 })}
								>
									<div
										style={{
											width: `${max > 0 ? (100 * row.values[ti]) / max : 0}%`,
											background: colors[ti],
											opacity: better(ti) ? 1 : 0.35,
										}}
									/>
								</div>
							))}
						</div>
					</div>
				);
			})}
		</div>
	);
};

// --- Around the game -----------------------------------------------------------

const injuryText = (injury: any) => {
	if (!injury || injury.type === "Healthy") {
		return undefined;
	}
	if (injury.gamesRemaining < 0) {
		return "Hurt this game";
	}
	if (injury.playingThrough) {
		return "Playing through it";
	}
	return injury.gamesRemaining > 0
		? `Out ${injury.gamesRemaining} ${helpers.plural("game", injury.gamesRemaining)}`
		: undefined;
};

export const Injuries = ({ boxScore }: { boxScore: any }) => {
	const lists = TEAM_NUMS_DISPLAY_ORDER.map((ti) => ({
		t: boxScore.teams[ti],
		hurt: (boxScore.teams[ti].players ?? []).filter(
			(p: any) => injuryText(p.injury) !== undefined,
		),
	}));
	if (lists.every((x) => x.hurt.length === 0)) {
		return null;
	}
	return (
		<Section title="Injuries">
			<table className="table table-sm gamecast-table">
				{lists.map(({ t, hurt }) =>
					hurt.length > 0 ? (
						<tbody key={t.abbrev}>
							<tr className="gamecast-team-head">
								<th colSpan={3}>
									<div className="d-flex align-items-center gap-2">
										<Logo t={t} size={22} />
										{t.region} {t.name}
									</div>
								</th>
							</tr>
							{hurt.map((p: any) => (
								<tr key={p.pid}>
									<td>
										<div className="d-flex align-items-center gap-2">
											<Headshot
												pid={p.pid}
												season={boxScore.season}
												size={28}
											/>
											<div className="text-truncate">
												<a href={playerUrl(p.pid)}>{p.name}</a>{" "}
												<span className="text-body-secondary">{p.pos}</span>
											</div>
										</div>
									</td>
									<td>{p.injury.type}</td>
									<td
										className={clsx({
											"text-danger": p.injury.gamesRemaining !== 0,
										})}
									>
										{injuryText(p.injury)}
									</td>
								</tr>
							))}
						</tbody>
					) : null,
				)}
			</table>
		</Section>
	);
};

const teamByTid = (boxScore: any, tid: number) =>
	boxScore.teams.find((t: any) => t.tid === tid);

const boxScoreUrl = (t: any, season: number, gid: number) =>
	helpers.leagueUrl(["game_log", `${t.abbrev}_${t.tid}`, season, gid]);

export const Series = ({
	boxScore,
	info,
}: {
	boxScore: any;
	info: GamecastInfo;
}) => {
	if (info.series.length === 0) {
		return null;
	}
	// The record so far, away team first as everywhere here.
	const [away, home] = TEAM_NUMS_DISPLAY_ORDER.map((ti) => boxScore.teams[ti]);
	const wins = (tid: number) =>
		info.series.filter((s) => {
			if (!s.pts) {
				return false;
			}
			const i = s.tids[0] === tid ? 0 : 1;
			return s.pts[i] > s.pts[i === 0 ? 1 : 0];
		}).length;
	const w: [number, number] = [wins(away.tid), wins(home.tid)];
	const summary =
		w[0] === w[1]
			? `Tied ${w[0]}-${w[1]}`
			: `${w[0] > w[1] ? away.abbrev : home.abbrev} leads ${Math.max(w[0], w[1])}-${Math.min(w[0], w[1])}`;
	return (
		<Section
			title={boxScore.playoffs ? "Series" : "Season Series"}
			action={<span className="text-body-secondary">{summary}</span>}
		>
			<table className="table table-sm gamecast-table gamecast-series">
				<tbody>
					{info.series.map((s, i) => {
						const homeT = teamByTid(boxScore, s.tids[0]);
						const awayT = teamByTid(boxScore, s.tids[1]);
						const won = (side: 0 | 1) =>
							s.pts !== undefined && s.pts[side] > s.pts[side === 0 ? 1 : 0];
						const score = s.pts ? (
							<>
								<span className={clsx({ "fw-bold": won(1) })}>{s.pts[1]}</span>
								{" - "}
								<span className={clsx({ "fw-bold": won(0) })}>{s.pts[0]}</span>
							</>
						) : null;
						return (
							<tr key={s.gid} className={clsx({ "table-warning": s.now })}>
								<td className="text-nowrap">
									Game {i + 1}
									{s.day !== undefined ? (
										<span className="text-body-secondary d-none d-sm-inline">
											{" "}
											(day {s.day})
										</span>
									) : null}
								</td>
								<td>
									<div className="d-flex align-items-center justify-content-center gap-2">
										{awayT ? <Logo t={awayT} size={20} /> : null}
										<span className={clsx({ "fw-bold": won(1) })}>
											{awayT?.abbrev}
										</span>
										<span className="text-body-secondary">@</span>
										<span className={clsx({ "fw-bold": won(0) })}>
											{homeT?.abbrev}
										</span>
										{homeT ? <Logo t={homeT} size={20} /> : null}
									</div>
								</td>
								<td className="text-nowrap">
									{s.now && !s.pts ? (
										<span className="fw-bold">Now</span>
									) : s.pts && homeT ? (
										// (Its box score.)
										<a href={boxScoreUrl(homeT, boxScore.season, s.gid)}>
											{score}
										</a>
									) : (
										<span className="text-body-secondary">Upcoming</span>
									)}
								</td>
							</tr>
						);
					})}
				</tbody>
			</table>
		</Section>
	);
};

const streakText = (streak: number) =>
	streak > 0 ? `W${streak}` : streak < 0 ? `L${-streak}` : "-";

export const Standings = ({
	boxScore,
	info,
	final,
}: {
	boxScore: any;
	info: GamecastInfo;
	// Its box score: the standings that night (otherwise at the tip).
	final?: boolean;
}) => {
	if (info.standings.length === 0) {
		return null;
	}
	const playing = new Set(boxScore.teams.map((t: any) => t.tid));
	return (
		<Section
			title="Standings"
			action={
				<span className="text-body-secondary">
					{final ? "After this game" : "Before this game"}
				</span>
			}
		>
			<table className="table table-sm gamecast-table gamecast-standings">
				{info.standings.map((conf) => (
					<tbody key={conf.name}>
						<tr className="gamecast-team-head">
							<th>{conf.name}</th>
							<th>W</th>
							<th>L</th>
							<th>PCT</th>
							<th>GB</th>
							<th>STRK</th>
						</tr>
						{conf.teams.map((t) => {
							const gp = t.won + t.lost + t.tied + t.otl;
							return (
								<tr
									key={t.tid}
									className={clsx({ "table-warning": playing.has(t.tid) })}
								>
									<td>
										<a
											className="d-flex align-items-center gap-2 text-reset text-decoration-none"
											href={helpers.leagueUrl([
												"roster",
												`${t.abbrev}_${t.tid}`,
												boxScore.season,
											])}
										>
											<span className="gamecast-rank">{t.rank}</span>
											<Logo t={t} size={20} />
											<span className="text-truncate">{t.region}</span>
										</a>
									</td>
									<td>{t.won}</td>
									<td>{t.lost}</td>
									<td>
										{gp > 0
											? helpers.roundWinp((t.won + 0.5 * t.tied) / gp)
											: "-"}
									</td>
									<td>{t.gb === 0 ? "-" : t.gb}</td>
									<td>{streakText(t.streak)}</td>
								</tr>
							);
						})}
					</tbody>
				))}
			</table>
			<div className="text-center">
				<a href={helpers.leagueUrl(["standings", boxScore.season])}>
					Full standings
				</a>
			</div>
		</Section>
	);
};

// Each team's last few games: who against, the result, each a link to its
// box score.
export const RecentGames = ({
	boxScore,
	info,
}: {
	boxScore: any;
	info: GamecastInfo;
}) => {
	const lists = TEAM_NUMS_DISPLAY_ORDER.map((ti) => {
		const t = boxScore.teams[ti];
		return { t, games: info.recent?.find((r) => r.tid === t.tid)?.games ?? [] };
	}).filter((x) => x.games.length > 0);
	if (lists.length === 0) {
		return null;
	}

	return (
		<Section title="Recent Games">
			<table className="table table-sm gamecast-table">
				{lists.map(({ t, games }) => (
					<tbody key={t.tid}>
						<tr className="gamecast-team-head">
							<th colSpan={3}>
								<div className="d-flex align-items-center gap-2">
									<Logo t={t} size={20} />
									{t.region} {t.name}
								</div>
							</th>
						</tr>
						{games.map((gm) => {
							const result =
								gm.pts[0] > gm.pts[1] ? "W" : gm.pts[0] < gm.pts[1] ? "L" : "T";
							return (
								<tr key={gm.gid} className={clsx({ "table-warning": gm.now })}>
									<td>
										{gm.home ? "vs" : "@"}{" "}
										{teamByTid(boxScore, gm.opp)?.abbrev ?? gm.oppAbbrev}
									</td>
									<td
										className={clsx("fw-bold", {
											"text-success": result === "W",
											"text-danger": result === "L",
										})}
									>
										{result}
									</td>
									<td>
										<a href={boxScoreUrl(t, boxScore.season, gm.gid)}>
											{gm.pts[0]}-{gm.pts[1]}
										</a>
									</td>
								</tr>
							);
						})}
					</tbody>
				))}
			</table>
		</Section>
	);
};

// --- The page ------------------------------------------------------------------

export const LiveGamecast = ({
	boxScore,
	entries,
	court,
	controls,
	boxScoreTab,
	header,
	onReplay,
	clipGame,
	shots,
	cursor,
}: {
	boxScore: any;
	entries: readonly FeedEntry[];
	// The 3D court, and the play / pause / speed controls laid over it.
	court: ReactNode;
	controls: ReactNode;
	// The full box score, for its tab.
	boxScoreTab: ReactNode;
	// Anything over the court (the replay label).
	header?: ReactNode;
	// Show a play again on the court (a highlight tapped) - where the court
	// can.
	onReplay?: (cursor: number) => void;
	// The game, for a highlight played right there under its row (see
	// HighlightClip).
	clipGame?: ClipGame;
	// The game's shots where the 3D court staged them, and how far into the
	// game it is (only those already shown).
	shots?: ShotSpot[];
	cursor?: number;
}) => {
	const [tab, setTab] = useState<Tab>("gamecast");
	const teams = boxScore.teams as [any, any];
	const season: number = boxScore.season;
	const numPeriods = boxScore.numPeriods ?? 4;
	// (Rebuilt every play: a few hundred rows at most.)
	const rows = feedRows(entries);
	const latest = rows.find((row) => !row.textOnly) ?? rows[0];
	const info: GamecastInfo | undefined = boxScore.gamecast;
	const colorKey = `${teams[0]?.colors?.join()}|${teams[1]?.colors?.join()}`;
	// eslint-disable-next-line react-hooks/exhaustive-deps
	const colors = useMemo(() => sideColors(teams), [colorKey]);

	const wrapRef = useRef<HTMLDivElement>(null);
	const tabsRef = useRef<HTMLDivElement>(null);
	// A new tab starts at its top: scrolled down a long Gamecast, the box
	// score would otherwise open at its foot.
	const toTab = (next: Tab) => {
		setTab(next);
		const wrap = wrapRef.current;
		const tabs = tabsRef.current;
		if (!wrap || !tabs) {
			return;
		}
		const NAVBAR = 52;
		const pinned =
			wrap.firstElementChild &&
			getComputedStyle(wrap.firstElementChild).position === "sticky";
		if (pinned) {
			const top = wrap.getBoundingClientRect().top + window.scrollY - NAVBAR;
			if (window.scrollY > top) {
				window.scrollTo({ top });
			}
		} else if (tabs.getBoundingClientRect().top < NAVBAR) {
			window.scrollTo({
				top: tabs.getBoundingClientRect().top + window.scrollY - NAVBAR - 8,
			});
		}
	};

	// The plays said as a broadcast says them, and each one, tapped, shown
	// again on the court (where it can be).
	const onReplayRef = useRef(onReplay);
	onReplayRef.current = onReplay;
	const canReplay = onReplay !== undefined;
	// A play opened under its row (see HighlightClip): the game as far as it
	// has got when it is opened.
	const clipRef = useRef({ clipGame, cursor, entries });
	clipRef.current = { clipGame, cursor, entries };
	const canClip = clipGame !== undefined;
	const names = useMemo(() => {
		const m = new Map<number, string>();
		for (const t of teams) {
			for (const p of t.players ?? []) {
				m.set(p.pid, p.name);
			}
		}
		return m;
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [boxScore.gid]);
	const feed = useMemo(
		() => ({
			say: (row: FeedRow) =>
				playSentence({
					events: row.events as any[],
					finish: row.finish,
					name: (pid) => (pid === undefined ? undefined : names.get(pid)),
					teamName: (t) =>
						teams[t] ? `${teams[t].region} ${teams[t].name}` : undefined,
				}),
			open: !canReplay
				? undefined
				: (row: FeedRow) => {
						if (row.cursor === undefined) {
							return;
						}
						const wrap = wrapRef.current;
						const pin = wrap?.firstElementChild;
						if (
							wrap &&
							pin &&
							getComputedStyle(pin).position !== "sticky" &&
							wrap.getBoundingClientRect().top < 0
						) {
							window.scrollTo({
								top: wrap.getBoundingClientRect().top + window.scrollY - 60,
								behavior: "smooth",
							});
						}
						onReplayRef.current?.(row.cursor);
					},
			clip: !canClip
				? undefined
				: (row: FeedRow) => {
						const {
							clipGame: game,
							cursor: upTo,
							entries: lines,
						} = clipRef.current;
						return game ? (
							<HighlightClip
								game={{ ...game, upTo }}
								first={lines.find((e) => e.key === row.key - 1)?.cursor ?? 0}
								last={row.cursor!}
							/>
						) : null;
					},
		}),
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[names, canReplay, canClip],
	);

	const shown =
		shots && cursor !== undefined
			? shots.filter((x) => x.cursor <= cursor)
			: undefined;
	const shotChart =
		shown && shown.length > 0 ? (
			<Section title="Shot Chart">
				<ShotChart
					spots={shown}
					teams={teams}
					colors={colors}
					names={names}
					plays={entries}
					numPeriods={numPeriods}
				/>
			</Section>
		) : null;

	return (
		<FeedContext.Provider value={feed}>
			<div className="gamecast" ref={wrapRef}>
				<div className="gamecast-pin">
					{header}
					<div className="row g-3">
						<div className="col-lg-8">
							<div className="gamecast-court">
								{court}
								<div className="gamecast-controls">{controls}</div>
							</div>
							<div className="gamecast-latest">
								{latest ? (
									<PlayRow
										row={latest}
										teams={teams}
										season={season}
										numPeriods={numPeriods}
										latest
									/>
								) : null}
							</div>
						</div>
						<div className="col-lg-4 d-none d-lg-block">
							<Linescore boxScore={boxScore} />
							<OnCourt boxScore={boxScore} />
						</div>
					</div>
					<div className="gamecast-tabs" role="tablist" ref={tabsRef}>
						{TABS.map(([key, label]) => (
							<button
								key={key}
								type="button"
								role="tab"
								aria-selected={tab === key}
								className={clsx({ active: tab === key })}
								onClick={() => toTab(key)}
							>
								{label}
							</button>
						))}
					</div>
				</div>
				<div className="gamecast-content">
					{tab === "gamecast" ? (
						<div className="gamecast-grid">
							<div>
								<Section
									title="Plays"
									action={
										<LinkButton onClick={() => toTab("plays")}>
											Full play-by-play
										</LinkButton>
									}
								>
									<PlayList
										rows={rows.slice(0, 6)}
										teams={teams}
										season={season}
										numPeriods={numPeriods}
									/>
								</Section>
								<Highlights
									rows={rows}
									teams={teams}
									season={season}
									numPeriods={numPeriods}
								/>
								<Section
									title="On the Court"
									className="d-lg-none"
									action={
										<LinkButton onClick={() => toTab("box")}>
											Box score
										</LinkButton>
									}
								>
									<Linescore boxScore={boxScore} />
									<OnCourt boxScore={boxScore} />
								</Section>
								<Section title="Game Leaders">
									<Leaders boxScore={boxScore} />
								</Section>
								<Injuries boxScore={boxScore} />
							</div>
							<div>
								<Section title="Game Flow">
									<FlowCharts boxScore={boxScore} colors={colors} live />
								</Section>
								{shotChart}
								<Section
									title="Team Stats"
									action={
										<LinkButton onClick={() => toTab("stats")}>
											Full team stats
										</LinkButton>
									}
								>
									<TeamStatBars boxScore={boxScore} colors={colors} />
								</Section>
								{info ? (
									<>
										<Series boxScore={boxScore} info={info} />
										<Standings boxScore={boxScore} info={info} />
									</>
								) : null}
							</div>
						</div>
					) : tab === "box" ? (
						boxScoreTab
					) : tab === "plays" ? (
						<div className="gamecast-narrow">
							{shotChart}
							<AllPlays
								rows={rows}
								teams={teams}
								season={season}
								numPeriods={numPeriods}
							/>
						</div>
					) : tab === "stats" ? (
						<div className="gamecast-narrow">
							<Section title="Team Stats">
								<TeamStatBars boxScore={boxScore} colors={colors} full />
							</Section>
						</div>
					) : (
						<Section title="Game Flow">
							<FlowCharts boxScore={boxScore} colors={colors} live />
						</Section>
					)}
				</div>
			</div>
		</FeedContext.Provider>
	);
};
