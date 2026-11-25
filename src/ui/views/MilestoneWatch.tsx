import type { ReactNode } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import type { View } from "../../common/types.ts";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { RailCard } from "../components/LeagueHub/HubRail.tsx";
import { usePlayerFace } from "../util/playerFaces.ts";
import { PlayerPicture } from "../components/PlayerPicture.tsx";
import type {
	ChaseRow,
	PaceRow,
	RoundRow,
	WatchStat,
} from "../../worker/util/milestoneWatch.ts";
import {
	STAT_ABBREVS,
	STAT_TITLES,
	STAT_WORDS,
	chaseNote,
	gamesAway,
	n,
	paceNote,
	playerUrl,
} from "../components/MilestoneNotes.tsx";

// WHO IS ABOUT TO PASS WHOM: the active players high on the league's
// all-time lists and the man each one is chasing, the ones a few games from a
// round number, and the seasons on pace to be among the league's best.

type Props = View<"milestoneWatch">;

const Face = ({ pid, season }: { pid: number; season: number }) => {
	const { lid } = useLocal(["lid"]);
	const face = usePlayerFace(pid, season, lid);
	return (
		<a
			className="lh-face-link"
			href={playerUrl(pid)}
			tabIndex={-1}
			aria-hidden="true"
		>
			<div className="lh-face mw-face">
				{face && (face.face || face.imgURL) ? (
					<PlayerPicture
						face={face.face}
						imgURL={face.imgURL}
						colors={face.colors}
						jersey={face.jersey}
						lazy
					/>
				) : null}
			</div>
		</a>
	);
};

const Team = ({ tid }: { tid: number }) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const t = teamInfoCache[tid];
	if (!t) {
		return <span className="lh-perf-team">Free agent</span>;
	}
	const url = t.imgURLSmall ?? t.imgURL;
	return (
		<span className="lh-perf-team">
			{url ? (
				<img
					className="lh-logo"
					src={url}
					alt=""
					width={16}
					height={16}
					style={{ objectFit: "contain" }}
				/>
			) : null}{" "}
			{t.abbrev}
		</span>
	);
};

const Row = ({
	pid,
	name,
	tid,
	season,
	lead,
	note,
	figure,
	figureLabel,
}: {
	pid: number;
	name: string;
	tid: number;
	season: number;
	lead?: ReactNode;
	note: ReactNode;
	figure: string;
	figureLabel: string;
}) => (
	<li className="mw-row">
		{lead !== undefined ? <span className="mw-rank">{lead}</span> : null}
		<Face pid={pid} season={season} />
		<div className="lh-perf-main">
			<div className="lh-perf-name">
				<a href={playerUrl(pid)}>{name}</a>
			</div>
			<Team tid={tid} />
			<div className="mw-note">{note}</div>
		</div>
		<div className="mw-figure">
			<b>{figure}</b>
			<span>{figureLabel}</span>
		</div>
	</li>
);

const Chase = ({
	stat,
	rows,
	season,
}: {
	stat: WatchStat;
	rows: ChaseRow[];
	season: number;
}) => (
	<RailCard title={STAT_TITLES[stat]}>
		<ol className="lh-list">
			{rows.map((row) => (
				<Row
					key={row.pid}
					pid={row.pid}
					name={row.name}
					tid={row.tid}
					season={season}
					lead={row.rank}
					note={chaseNote(row)}
					figure={n(row.total)}
					figureLabel={STAT_ABBREVS[stat]}
				/>
			))}
		</ol>
	</RailCard>
);

const Round = ({ rows, season }: { rows: RoundRow[]; season: number }) => (
	<RailCard title="Round numbers" className="lh-o2">
		<ul className="lh-list">
			{rows.map((row) => (
				<Row
					key={`${row.pid}:${row.stat}`}
					pid={row.pid}
					name={row.name}
					tid={row.tid}
					season={season}
					note={`${n(row.total)} career ${STAT_WORDS[row.stat]}. ${gamesAway(row.games)}`}
					figure={n(row.mark - row.total)}
					figureLabel={`to ${n(row.mark)}`}
				/>
			))}
		</ul>
	</RailCard>
);

const Pace = ({ rows, season }: { rows: PaceRow[]; season: number }) => (
	<RailCard title="Record pace" className="lh-o1">
		<ul className="lh-list">
			{rows.map((row) => (
				<Row
					key={`${row.pid}:${row.stat}`}
					pid={row.pid}
					name={row.name}
					tid={row.tid}
					season={season}
					note={paceNote(row)}
					figure={n(row.projected)}
					figureLabel={`${STAT_ABBREVS[row.stat]} pace`}
				/>
			))}
		</ul>
	</RailCard>
);

const MilestoneWatch = ({ chase, pace, round, season }: Props) => {
	useTitleBar({ title: "Milestone Watch" });

	const lists = chase.filter((c) => c.rows.length > 0);
	if (lists.length === 0 && pace.length === 0 && round.length === 0) {
		return <p>Nothing to watch yet.</p>;
	}

	return (
		<div className="lh-grid mw">
			<div className="lh-main">
				{lists.length > 0 ? (
					<section className="lh-o3">
						<h2 className="lh-section-title">
							All-time lists{" "}
							<a
								className="mw-more"
								href={helpers.leagueUrl([
									"leaders",
									"career",
									"totals",
									"regularSeason",
								])}
							>
								Career leaders
							</a>
						</h2>
						<div className="mw-lists">
							{lists.map((c) => (
								<Chase
									key={c.stat}
									stat={c.stat}
									rows={c.rows}
									season={season}
								/>
							))}
						</div>
					</section>
				) : null}
			</div>
			<div className="lh-rail">
				{pace.length > 0 ? <Pace rows={pace} season={season} /> : null}
				{round.length > 0 ? <Round rows={round} season={season} /> : null}
			</div>
		</div>
	);
};

export default MilestoneWatch;
