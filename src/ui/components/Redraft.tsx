import clsx from "clsx";
import { useState } from "react";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { usePlayerFace } from "../util/playerFaces.ts";
import { PlayerPicture } from "./PlayerPicture.tsx";
import { getCol } from "../../common/getCol.ts";

// THE FIRST ROUND DONE AGAIN, knowing how the careers went: each pick of the
// first round, the team that made it, and who it would take now by career
// value - with where he actually went.

type RedraftPlayer = {
	pid: number;
	firstName: string;
	lastName: string;
	draft: { round: number; pick: number; tid: number };
	redraft: number | undefined;
	careerStats: Record<string, any>;
};

type DraftTeam = {
	abbrev: string;
	imgURL?: string;
	imgURLSmall?: string;
};

const SHOWN = 10;

const Face = ({ pid, season }: { pid: number; season: number }) => {
	const { lid } = useLocal(["lid"]);
	const face = usePlayerFace(pid, season, lid);
	return (
		<div className="rd-face">
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
	);
};

// Where he really went, against his place in the redraft: a first-round pick
// with how far he moves ("Went 14th", up 13), a later one by round and pick
// ("Went 2-3"), or "Undrafted".
export const Moved = ({
	draft,
	redraft,
}: {
	draft: { round: number; pick: number };
	redraft: number;
}) => {
	if (draft.round < 1) {
		return <span className="rd-moved rd-later">Undrafted</span>;
	}
	if (draft.round > 1) {
		return (
			<span className="rd-moved rd-later">
				Went {draft.round}-{draft.pick}
			</span>
		);
	}
	if (draft.pick === redraft) {
		return <span className="rd-moved">Same pick</span>;
	}
	const up = draft.pick > redraft;
	return (
		<span className={clsx("rd-moved", up ? "rd-up" : "rd-down")}>
			Went {helpers.ordinal(draft.pick)}{" "}
			<span className="rd-arrow">
				{up ? "▲" : "▼"}
				{Math.abs(draft.pick - redraft)}
			</span>
		</span>
	);
};

export const Redraft = ({
	players,
	season,
	summaryStat,
	teamsByTid,
}: {
	players: RedraftPlayer[];
	season: number;
	summaryStat: string;
	teamsByTid: Record<number, DraftTeam | undefined>;
}) => {
	const [all, setAll] = useState(false);

	// The first round's picks, and the team that made each.
	const slots = players.filter((p) => p.draft.round === 1);
	const byRedraft = new Map(
		players.filter((p) => p.redraft !== undefined).map((p) => [p.redraft!, p]),
	);
	if (slots.length === 0 || byRedraft.size === 0) {
		return null;
	}

	// Every pick of the round that has someone to take, with the team that
	// made it where the league still has the man it took.
	const picks = Array.from(
		{ length: Math.max(...slots.map((slot) => slot.draft.pick)) },
		(_, i) => ({
			pick: i + 1,
			tid: slots.find((slot) => slot.draft.pick === i + 1)?.draft.tid,
		}),
	).filter(({ pick }) => byRedraft.has(pick));

	const col = getCol(`stat:${summaryStat}`);
	const shown = all ? picks : picks.slice(0, SHOWN);

	return (
		<section className="rd lh-card">
			<div className="lh-card-head">
				<h2 className="lh-card-title">
					Redraft <span className="rd-by">by career {col.title}</span>
				</h2>
			</div>
			<ol
				className="rd-list"
				style={{
					gridTemplateRows: `repeat(${Math.ceil(shown.length / 2)}, auto)`,
				}}
			>
				{shown.map(({ pick, tid }) => {
					const p = byRedraft.get(pick);
					const t = tid === undefined ? undefined : teamsByTid[tid];
					const logo = t?.imgURLSmall ?? t?.imgURL;
					return (
						<li key={pick} className="rd-row">
							<span className="rd-pick">{pick}</span>
							<span className="rd-team">
								{logo ? <img src={logo} alt="" width={24} height={24} /> : null}
								<span>{t?.abbrev}</span>
							</span>
							{p ? (
								<>
									<a
										className="rd-face-link"
										href={helpers.leagueUrl(["player", p.pid])}
										tabIndex={-1}
										aria-hidden="true"
									>
										<Face pid={p.pid} season={season} />
									</a>
									<span className="rd-main">
										<a
											className="rd-name"
											href={helpers.leagueUrl(["player", p.pid])}
										>
											{p.firstName} {p.lastName}
										</a>
										<Moved draft={p.draft} redraft={pick} />
									</span>
									<span className="rd-value">
										<b>
											{helpers.roundStat(
												p.careerStats[summaryStat] ?? 0,
												summaryStat,
											)}
										</b>
										<span>{col.title}</span>
									</span>
								</>
							) : null}
						</li>
					);
				})}
			</ol>
			{picks.length > SHOWN ? (
				<button
					className="btn btn-link btn-sm rd-toggle"
					onClick={() => setAll(!all)}
				>
					{all ? "Top 10" : "Whole first round"}
				</button>
			) : null}
		</section>
	);
};
