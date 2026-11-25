import { useEffect, useState } from "react";
import { toWorker } from "../../util/toWorker.ts";
import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";
import HideableSection from "../../components/HideableSection.tsx";
import {
	STAT_TITLES,
	STAT_WORDS,
	chaseNote,
	gamesAway,
	n,
	paceNote,
} from "../../components/MilestoneNotes.tsx";
import type { getPlayerAllTime } from "../../../worker/util/milestoneWatch.ts";

// A player's place on each of the league's all-time lists, for anyone who has
// played: his rank and career total in each stat, each opening that list.
// Still playing, who he passes next, the round number he's closest to and a
// season on a record pace. Worked out after the page is up, since it reads
// the whole league.

type Data = Awaited<ReturnType<typeof getPlayerAllTime>>;

const Milestones = ({ pid }: { pid: number }) => {
	const { gameSimInProgress } = useLocal(["gameSimInProgress"]);
	const [data, setData] = useState<{ pid: number; value: Data } | undefined>();

	useEffect(() => {
		if (gameSimInProgress) {
			return;
		}
		let cancelled = false;
		(async () => {
			const value = await toWorker("main", "getPlayerAllTime", pid);
			if (!cancelled) {
				setData({ pid, value });
			}
		})();
		return () => {
			cancelled = true;
		};
	}, [pid, gameSimInProgress]);

	const value = data?.pid === pid ? data.value : undefined;
	if (!value) {
		return null;
	}

	const { round, pace } = value;

	return (
		<HideableSection
			title="All-Time Ranks"
			description={
				<a href={helpers.leagueUrl(["milestone_watch"])}>Milestone Watch</a>
			}
		>
			<div className="atr-grid">
				{value.ranks.map((row) => (
					<div key={row.stat} className="atr-tile">
						<a
							className="atr-head"
							href={helpers.leagueUrl(["all_time_leaders", row.stat, pid])}
						>
							<span className="atr-rank">
								{row.rank === undefined ? "-" : helpers.ordinal(row.rank)}
							</span>
							<span className="atr-stat">{STAT_TITLES[row.stat]}</span>
						</a>
						<div className="atr-total">
							{n(row.total)}
							{row.rank !== undefined ? (
								<span className="atr-of"> · of {n(row.of)}</span>
							) : null}
						</div>
						{value.active && row.rank !== undefined ? (
							<div className="atr-note">
								{chaseNote({ ...row, rank: row.rank })}
							</div>
						) : null}
					</div>
				))}
			</div>
			{round || pace ? (
				<ul className="atr-extra">
					{round ? (
						<li>
							<b>{n(round.mark - round.total)}</b> to {n(round.mark)}{" "}
							{STAT_WORDS[round.stat]}
							{round.games !== undefined ? (
								<span className="atr-extra-note">
									{" "}
									· {gamesAway(round.games)}
								</span>
							) : null}
						</li>
					) : null}
					{pace ? (
						<li>
							On pace for <b>{n(pace.projected)}</b> {STAT_WORDS[pace.stat]}{" "}
							this season
							<span className="atr-extra-note"> · {paceNote(pace)}</span>
						</li>
					) : null}
				</ul>
			) : null}
		</HideableSection>
	);
};

export default Milestones;
