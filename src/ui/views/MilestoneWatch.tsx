import clsx from "clsx";
import { useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import type { View } from "../../common/types.ts";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { usePlayerFace } from "../util/playerFaces.ts";
import { PlayerPicture } from "../components/PlayerPicture.tsx";
import type {
	ChaseRow,
	PaceRow,
	RoundRow,
	WatchStat,
} from "../../worker/util/milestoneWatch.ts";
import {
	careerLeadersUrl,
	STAT_ABBREVS,
	STAT_TITLES,
	STAT_WORDS,
	n,
	playerUrl,
} from "../components/MilestoneNotes.tsx";

// WHO IS ABOUT TO PASS WHOM: the active players a few games from a round
// number, the seasons on pace to be among the league's best, and the active
// players high on the league's all-time lists with the man each is chasing.

type Props = View<"milestoneWatch">;

const splitName = (name: string): [string, string] => {
	const i = name.indexOf(" ");
	return i < 0 ? ["", name] : [name.slice(0, i), name.slice(i + 1)];
};

const useTeam = (tid: number, season: number) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const t = teamInfoCache[tid];
	if (!t) {
		return undefined;
	}
	return {
		abbrev: t.abbrev,
		logo: t.imgURLSmall ?? t.imgURL,
		url: helpers.leagueUrl(["roster", `${t.abbrev}_${tid}`, season]),
	};
};

// His picture, drawn as everywhere else in the game: his photo, his face, or
// (a real player with neither) the blank silhouette.
const Avatar = ({ pid, season }: { pid: number; season: number }) => {
	const { lid } = useLocal(["lid"]);
	const face = usePlayerFace(pid, season, lid);
	return (
		<a
			className="mw-avatar"
			href={playerUrl(pid)}
			tabIndex={-1}
			aria-hidden="true"
		>
			{face && (face.face || face.imgURL) ? (
				<PlayerPicture
					face={face.face}
					imgURL={face.imgURL}
					colors={face.colors}
					jersey={face.jersey}
					lazy
				/>
			) : null}
		</a>
	);
};

const TeamTag = ({ tid, season }: { tid: number; season: number }) => {
	const team = useTeam(tid, season);
	if (!team) {
		return <span className="mw-team">FA</span>;
	}
	return (
		<a className="mw-team" href={team.url}>
			{team.logo ? <img src={team.logo} alt="" width={16} height={16} /> : null}
			{team.abbrev}
		</a>
	);
};

const Head = ({
	title,
	more,
}: {
	title: string;
	more?: { href: string; text: string };
}) => (
	<div className="mw-head">
		<h2 className="mw-title">{title}</h2>
		{more ? (
			<a className="mw-more" href={more.href}>
				{more.text}
			</a>
		) : null}
	</div>
);

const eta = (games: number | undefined) =>
	games === undefined ? "" : games <= 1 ? "Next game" : `~${games} games`;

// (Built when it's shown: the league isn't known yet when this file loads.)
const careerLeaders = () =>
	helpers.leagueUrl(["leaders", "career", "totals", "regularSeason"]);

const recordNote = (row: PaceRow) =>
	row.record.pid === row.pid ? (
		<>His own, {row.record.season}</>
	) : (
		<>
			<a href={playerUrl(row.record.pid)}>{row.record.name}</a>,{" "}
			{row.record.season}
		</>
	);

const PlayerCell = ({
	pid,
	name,
	tid,
	season,
}: {
	pid: number;
	name: string;
	tid: number;
	season: number;
}) => (
	<div className="mw-player">
		<Avatar pid={pid} season={season} />
		<div className="mw-player-text">
			<a className="mw-name" href={playerUrl(pid)}>
				{name}
			</a>
			<TeamTag tid={tid} season={season} />
		</div>
	</div>
);

const RoundTable = ({ rows, season }: { rows: RoundRow[]; season: number }) => (
	<section className="mw-sec">
		<Head title="Next milestones" />
		<table className="mw-table">
			<thead>
				<tr>
					<th>Player</th>
					<th>Milestone</th>
					<th className="mw-num mw-wide">Career</th>
					<th className="mw-num">Needs</th>
					<th className="mw-wide">Expected</th>
				</tr>
			</thead>
			<tbody>
				{rows.map((row) => (
					<tr key={`${row.pid}:${row.stat}`}>
						<td>
							<PlayerCell
								pid={row.pid}
								name={row.name}
								tid={row.tid}
								season={season}
							/>
						</td>
						<td>
							<b>{n(row.mark)}</b> {STAT_WORDS[row.stat]}
							<div className="mw-sub mw-narrow">{n(row.total)} now</div>
						</td>
						<td className="mw-num mw-wide">{n(row.total)}</td>
						<td className="mw-num">
							<span className="mw-big">{n(row.mark - row.total)}</span>
							<div className="mw-sub mw-narrow">{eta(row.games)}</div>
						</td>
						<td className="mw-wide">{eta(row.games)}</td>
					</tr>
				))}
			</tbody>
		</table>
	</section>
);

const PaceTable = ({ rows, season }: { rows: PaceRow[]; season: number }) => (
	<section className="mw-sec">
		<Head title="Record pace" />
		<table className="mw-table">
			<thead>
				<tr>
					<th>Player</th>
					<th className="mw-wide">Stat</th>
					<th className="mw-num mw-wide">So far</th>
					<th className="mw-num">Pace</th>
					<th className="mw-num">All-time</th>
					<th className="mw-wide">Record</th>
				</tr>
			</thead>
			<tbody>
				{rows.map((row) => (
					<tr key={`${row.pid}:${row.stat}`}>
						<td>
							<PlayerCell
								pid={row.pid}
								name={row.name}
								tid={row.tid}
								season={season}
							/>
						</td>
						<td className="mw-wide">{STAT_TITLES[row.stat]}</td>
						<td className="mw-num mw-wide">{n(row.total)}</td>
						<td className="mw-num">
							<span className="mw-big">{n(row.projected)}</span>
							<div className="mw-sub mw-narrow">
								{STAT_ABBREVS[row.stat]}, {n(row.total)} so far
							</div>
						</td>
						<td className="mw-num">
							<span
								className={clsx("mw-place", row.place === 1 && "mw-place-1")}
							>
								{helpers.ordinal(row.place)}
							</span>
							<div className="mw-sub mw-narrow">
								Record {n(row.record.total)}
							</div>
						</td>
						<td className="mw-wide mw-record">
							<b>{n(row.record.total)}</b>{" "}
							<span className="mw-sub">{recordNote(row)}</span>
						</td>
					</tr>
				))}
			</tbody>
		</table>
	</section>
);

type Rung = {
	key: string;
	rank: number;
	pid: number;
	name: string;
	tid: number;
	total: number;
	// The active player himself, and his gap; or the man he's chasing.
	row?: ChaseRow;
};

// The active players, each with the man he's chasing (or, for the leader, the
// man behind him) a rung away, unless that man is one of them.
const rungs = (rows: ChaseRow[]) => {
	const shown = new Set(rows.map((row) => row.pid));
	const out: Rung[] = [];
	const add = (r: Rung) => {
		if (!out.some((x) => x.pid === r.pid)) {
			out.push(r);
		}
	};
	for (const row of rows) {
		if (row.next && !shown.has(row.next.pid)) {
			add({ key: `n${row.next.pid}`, rank: row.rank - 1, ...row.next });
		}
		add({ key: `p${row.pid}`, ...row, row });
		if (row.behind && !shown.has(row.behind.pid)) {
			add({ key: `b${row.behind.pid}`, rank: 2, ...row.behind });
		}
	}
	return out.sort((a, b) => a.rank - b.rank || b.total - a.total);
};

const gapNote = (row: ChaseRow) => {
	if (row.next) {
		return `${n(row.next.total - row.total + 1)} to pass ${splitName(row.next.name)[1]}`;
	}
	if (row.behind) {
		return `Leads by ${n(row.total - row.behind.total)}`;
	}
	return "All-time leader";
};

const Ladder = ({
	stat,
	rows,
	season,
	off,
}: {
	stat: WatchStat;
	rows: ChaseRow[];
	season: number;
	off: boolean;
}) => {
	const list = rungs(rows);
	return (
		<section className={clsx("mw-ladder", off && "mw-ladder-off")}>
			<Head
				title={STAT_TITLES[stat]}
				more={{ href: careerLeadersUrl(stat), text: "Full list" }}
			/>
			<ol className="mw-rungs">
				{list.map((r, i) => (
					<li
						key={r.key}
						className={clsx(
							"mw-rung",
							r.row ? "mw-rung-on" : "mw-rung-ghost",
							i > 0 && r.rank - (list[i - 1]?.rank ?? 0) > 1 && "mw-rung-gap",
						)}
					>
						<span className="mw-rung-rank">{r.rank}</span>
						<Avatar pid={r.pid} season={season} />
						<div className="mw-rung-main">
							<a className="mw-name" href={playerUrl(r.pid)}>
								{r.name}
							</a>
							{r.row ? (
								<div className="mw-rung-sub">
									<TeamTag tid={r.tid} season={season} />
									<span>{gapNote(r.row)}</span>
								</div>
							) : null}
						</div>
						<span className="mw-rung-total">{n(r.total)}</span>
					</li>
				))}
			</ol>
		</section>
	);
};

const Ladders = ({
	lists,
	season,
}: {
	lists: { stat: WatchStat; rows: ChaseRow[] }[];
	season: number;
}) => {
	const [picked, setPicked] = useState(lists[0]!.stat);
	const on = lists.some((c) => c.stat === picked) ? picked : lists[0]!.stat;
	return (
		<section className="mw-sec mw-ladders-wrap">
			<Head
				title="All-time lists"
				more={{ href: careerLeaders(), text: "Career leaders" }}
			/>
			<div className="mw-tabs" role="tablist">
				{lists.map((c) => (
					<button
						key={c.stat}
						type="button"
						role="tab"
						aria-selected={c.stat === on}
						className={clsx("mw-tab", c.stat === on && "mw-tab-on")}
						onClick={() => {
							setPicked(c.stat);
						}}
					>
						{STAT_ABBREVS[c.stat]}
					</button>
				))}
			</div>
			<div className="mw-ladders">
				{lists.map((c) => (
					<Ladder
						key={c.stat}
						stat={c.stat}
						rows={c.rows}
						season={season}
						off={c.stat !== on}
					/>
				))}
			</div>
		</section>
	);
};

const MilestoneWatch = ({ chase, pace, round, season }: Props) => {
	useTitleBar({ title: "Milestone Watch" });

	const lists = chase.filter((c) => c.rows.length > 0);
	if (lists.length === 0 && pace.length === 0 && round.length === 0) {
		return <p>Nothing to watch yet.</p>;
	}

	return (
		<div className="mw">
			{round.length > 0 ? <RoundTable rows={round} season={season} /> : null}
			{pace.length > 0 ? <PaceTable rows={pace} season={season} /> : null}
			{lists.length > 0 ? <Ladders lists={lists} season={season} /> : null}
		</div>
	);
};

export default MilestoneWatch;
