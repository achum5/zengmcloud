import useTitleBar from "../hooks/useTitleBar.tsx";
import type { View } from "../../common/types.ts";
import { PLAYER } from "../../common/constants.ts";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { usePlayerFace } from "../util/playerFaces.ts";
import { PlayerPicture } from "../components/PlayerPicture.tsx";
import {
	STAT_ABBREVS,
	STAT_TITLES,
	STAT_WORDS,
	n,
	playerUrl,
} from "../components/MilestoneNotes.tsx";
import type {
	AllTimeLeader,
	WatchStat,
} from "../../worker/util/milestoneWatch.ts";

// ONE ALL-TIME LIST: every player in the league's history by his career
// regular season total, the man the page was opened for marked.

type Props = View<"allTimeLeaders">;

// (In the Milestone Watch's order.)
const STATS = Object.keys(STAT_TITLES) as WatchStat[];

const Face = ({ pid }: { pid: number }) => {
	const { lid } = useLocal(["lid"]);
	const face = usePlayerFace(pid, undefined, lid);
	return (
		<a
			className="atl-face"
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

const Where = ({ row }: { row: AllTimeLeader }) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const span = row.from === row.to ? `${row.from}` : `${row.from}-${row.to}`;
	let team;
	if (!row.active) {
		team = "Retired";
	} else if (row.tid === PLAYER.FREE_AGENT) {
		team = "Free agent";
	} else {
		team = teamInfoCache[row.tid]?.abbrev ?? "";
	}
	return (
		<div className="atl-where">
			{team} · {span}
		</div>
	);
};

const Row = ({
	row,
	me,
	stat,
}: {
	row: AllTimeLeader;
	me: boolean;
	stat: Props["stat"];
}) => (
	<li className={me ? "atl-row atl-me" : "atl-row"}>
		<span className="atl-rank">{row.rank}</span>
		<Face pid={row.pid} />
		<div className="atl-main">
			<div className="atl-name">
				<a href={playerUrl(row.pid)}>{row.name}</a>
				{row.active ? <span className="atl-active">Active</span> : null}
			</div>
			<Where row={row} />
		</div>
		<div className="atl-figure">
			<b>{n(row.total)}</b>
			<span>
				{row.gp > 0 ? (row.total / row.gp).toFixed(1) : "-"}{" "}
				{STAT_ABBREVS[stat]}/G
			</span>
		</div>
	</li>
);

const AllTimeLeaders = ({ around, of, pid, stat, top }: Props) => {
	useTitleBar({ title: "All-Time Leaders" });

	const link = (s: Props["stat"]) =>
		helpers.leagueUrl(
			pid === undefined
				? ["all_time_leaders", s]
				: ["all_time_leaders", s, pid],
		);

	return (
		<>
			<nav className="atl-stats" aria-label="Stat">
				{STATS.map((s) => (
					<a
						key={s}
						href={link(s)}
						className={s === stat ? "atl-stat atl-stat-on" : "atl-stat"}
						aria-current={s === stat ? "page" : undefined}
					>
						{STAT_TITLES[s]}
					</a>
				))}
			</nav>
			<p className="atl-sub">
				Career {STAT_WORDS[stat]}, regular season · {n(of)} players
			</p>
			{top.length === 0 ? (
				<p>Nobody yet.</p>
			) : (
				<ol className="atl-list">
					{top.map((row) => (
						<Row key={row.pid} row={row} me={row.pid === pid} stat={stat} />
					))}
					{around.length > 0 ? (
						<>
							<li className="atl-gap" aria-hidden="true">
								⋯
							</li>
							{around.map((row) => (
								<Row key={row.pid} row={row} me={row.pid === pid} stat={stat} />
							))}
						</>
					) : null}
				</ol>
			)}
		</>
	);
};

export default AllTimeLeaders;
