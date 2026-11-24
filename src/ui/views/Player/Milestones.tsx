import { useEffect, useState, type ReactNode } from "react";
import { toWorker } from "../../util/toWorker.ts";
import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";
import {
	STAT_WORDS,
	chaseNote,
	gamesAway,
	paceNote,
} from "../../components/MilestoneNotes.tsx";
import type { getPlayerMilestones } from "../../../worker/util/milestoneWatch.ts";

// A player's part of the Milestone Watch: where he stands on the all-time
// lists he is high on, the round number he's closest to, and a season on a
// record pace. Worked out after the page is up, since it reads the whole
// league, and nothing shows when there's nothing to watch.

type Data = Awaited<ReturnType<typeof getPlayerMilestones>>;

const Milestones = ({ pid }: { pid: number }) => {
	const { gameSimInProgress } = useLocal(["gameSimInProgress"]);
	const [data, setData] = useState<{ pid: number; value: Data } | undefined>();

	useEffect(() => {
		if (gameSimInProgress) {
			return;
		}
		let cancelled = false;
		(async () => {
			const value = await toWorker("main", "getPlayerMilestones", pid);
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

	const n = helpers.numberWithCommas;
	const items: {
		key: string;
		figure: string;
		label: string;
		note: ReactNode;
	}[] = [];
	if (value.pace) {
		items.push({
			key: "pace",
			figure: n(value.pace.projected),
			label: `${STAT_WORDS[value.pace.stat]} pace`,
			note: paceNote(value.pace),
		});
	}
	for (const row of value.chase) {
		items.push({
			key: `chase:${row.stat}`,
			figure: helpers.ordinal(row.rank),
			label: `all-time in ${STAT_WORDS[row.stat]}`,
			note: chaseNote(row),
		});
	}
	if (value.round) {
		const r = value.round;
		items.push({
			key: "round",
			figure: n(r.mark - r.total),
			label: `to ${n(r.mark)} ${STAT_WORDS[r.stat]}`,
			note: gamesAway(r.games),
		});
	}

	return (
		<section className="pm">
			<div className="pm-head">
				<h2 className="pm-title">Milestone Watch</h2>
				<a href={helpers.leagueUrl(["milestone_watch"])}>All players</a>
			</div>
			<ul className="pm-items">
				{items.map((item) => (
					<li key={item.key} className="pm-item">
						<b className="pm-figure">{item.figure}</b>
						<span className="pm-label">{item.label}</span>
						<span className="pm-note">{item.note}</span>
					</li>
				))}
			</ul>
		</section>
	);
};

export default Milestones;
