import { useEffect, useState } from "react";
import { PLAYER } from "../../../common/constants.ts";
import {
	type AllTimeFormat,
	type AllTimeScope,
	allTimeStats,
} from "../../../common/allTimeStats.ts";
import { toWorker } from "../../util/toWorker.ts";
import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";
import { usePlayerFace } from "../../util/playerFaces.ts";
import { safeLocalStorage } from "../../util/safeLocalStorage.ts";
import HideableSection from "../../components/HideableSection.tsx";
import { PlayerPicture } from "../../components/PlayerPicture.tsx";
import {
	STAT_WORDS,
	gamesAway,
	n,
	paceNote,
	playerUrl,
} from "../../components/MilestoneNotes.tsx";
import type {
	AllTimeEntry,
	LadderRow,
	getPlayerAllTime,
} from "../../../worker/util/allTimeRanks.ts";

// A player's place on every all-time list, for anyone who has played: a
// picker of the lists with his rank on each, and the one picked shown the
// way a broadcast does, two men ahead of him and one behind. Worked out after
// the page is up, since it reads the whole league.

type Data = Awaited<ReturnType<typeof getPlayerAllTime>>;

const SCOPES: { key: AllTimeScope; label: string }[] = [
	{ key: "totals", label: "Totals" },
	{ key: "perGame", label: "Per Game" },
	{ key: "playoffs", label: "Playoffs" },
];

const NEEDS_WORDS: Record<string, string> = {
	gp: "games",
	fg: "field goals",
	tp: "3-pointers",
	ft: "free throws",
	min: "minutes",
};

const format = (value: number, how: AllTimeFormat) => {
	switch (how) {
		case "int":
			return n(Math.round(value));
		case "dec1":
			return value.toFixed(1);
		case "pct":
			return `${value.toFixed(1)}%`;
		case "dec3":
			return value.toFixed(3).replace(/^0\./, ".");
		case "signed1":
			return `${value > 0 ? "+" : ""}${value.toFixed(1)}`;
	}
};

// The list picked in each scope, kept as a viewer goes from player to player.
const PICKED_KEY = "allTimeRanksPicked";
const DEFAULT_PICKED: Record<AllTimeScope, string> = {
	totals: "pts",
	perGame: "ppg",
	playoffs: "pts",
};
const loadPicked = (): Record<AllTimeScope, string> => {
	try {
		return {
			...DEFAULT_PICKED,
			...JSON.parse(safeLocalStorage.getItem(PICKED_KEY) ?? "{}"),
		};
	} catch {
		return DEFAULT_PICKED;
	}
};

const fullListUrl = (scope: AllTimeScope, key: string) => {
	const stat = allTimeStats(scope).find((s) => s.key === key);
	if (scope === "perGame" || !stat?.table) {
		return;
	}
	return `${helpers.leagueUrl([
		"player_stats",
		"all",
		"career",
		stat.table,
		scope === "playoffs" ? "playoffs" : "regularSeason",
	])}?sort=${key}`;
};

const Face = ({ pid }: { pid: number }) => {
	const { lid } = useLocal(["lid"]);
	const face = usePlayerFace(pid, undefined, lid);
	return (
		<a
			className="atr-face"
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

const Rung = ({
	row,
	me,
	how,
}: {
	row: LadderRow;
	me: boolean;
	how: AllTimeFormat;
}) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const years = row.from === row.to ? `${row.from}` : `${row.from}-${row.to}`;
	let team;
	if (row.active) {
		team =
			row.tid === PLAYER.FREE_AGENT ? "FA" : teamInfoCache[row.tid]?.abbrev;
	}
	return (
		<li className={me ? "atr-rung atr-rung-me" : "atr-rung"}>
			<span className="atr-rung-rank">{row.rank}</span>
			<Face pid={row.pid} />
			<div className="atr-rung-main">
				<a className="atr-rung-name" href={playerUrl(row.pid)}>
					{row.name}
				</a>
				<div className="atr-rung-sub">
					{team ? `${team} · ` : null}
					{years}
				</div>
			</div>
			<span className="atr-rung-value">{format(row.value, how)}</span>
		</li>
	);
};

const Ladder = ({
	entry,
	pid,
	scope,
}: {
	entry: AllTimeEntry;
	pid: number;
	scope: AllTimeScope;
}) => {
	const stat = allTimeStats(scope).find((s) => s.key === entry.key)!;
	const title =
		scope === "totals"
			? `All-Time ${stat.title}`
			: scope === "playoffs"
				? `Playoff ${stat.title}`
				: stat.title;
	const url = fullListUrl(scope, entry.key);
	return (
		<div className="atr-ladder">
			<div className="atr-ladder-head">
				<span className="atr-ladder-title">{title}</span>
				{url ? <a href={url}>Full list</a> : null}
			</div>
			{entry.needs ? (
				<p className="atr-needs">
					{entry.value !== undefined ? (
						<b>{format(entry.value, stat.format)} </b>
					) : null}
					Needs {n(entry.needs.value)} {NEEDS_WORDS[entry.needs.stat]} to
					qualify, has {n(entry.needs.have)}
				</p>
			) : (
				<ol className="atr-ladder-list">
					{entry.ladder.map((row) => (
						<Rung
							key={row.pid}
							row={row}
							me={row.pid === pid}
							how={stat.format}
						/>
					))}
				</ol>
			)}
		</div>
	);
};

const Milestones = ({ pid }: { pid: number }) => {
	const { gameSimInProgress } = useLocal(["gameSimInProgress"]);
	const [data, setData] = useState<{ pid: number; value: Data } | undefined>();
	const [scope, setScope] = useState<AllTimeScope>("totals");
	const [picked, setPicked] = useState(loadPicked);

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
	// (Per Game only once he's on one of its lists.)
	const ranked = (key: AllTimeScope) =>
		value.scopes[key].some((entry) => entry.rank !== undefined);
	const scopes = SCOPES.filter((s) => ranked(s.key));
	const shownScope = ranked(scope) ? scope : "totals";
	const stats = allTimeStats(shownScope);

	// The lists he's on, or (a rate) could be.
	const entries = value.scopes[shownScope].filter(
		(entry) => entry.rank !== undefined || entry.needs,
	);
	const current =
		entries.find((entry) => entry.key === picked[shownScope]) ?? entries[0];

	const pick = (key: string) => {
		const next = { ...picked, [shownScope]: key };
		setPicked(next);
		safeLocalStorage.setItem(PICKED_KEY, JSON.stringify(next));
	};

	return (
		<HideableSection
			title="All-Time Ranks"
			description={
				<a href={helpers.leagueUrl(["milestone_watch"])}>Milestone Watch</a>
			}
		>
			<div className="atr-scopes" role="tablist">
				{scopes.map((s) => (
					<button
						key={s.key}
						type="button"
						role="tab"
						aria-selected={s.key === shownScope}
						className={
							s.key === shownScope ? "atr-scope atr-scope-on" : "atr-scope"
						}
						onClick={() => {
							setScope(s.key);
						}}
					>
						{s.label}
					</button>
				))}
			</div>
			<div className="atr-body">
				<div className="atr-picker" role="tablist">
					{entries.map((entry) => {
						const stat = stats.find((s) => s.key === entry.key)!;
						const on = entry === current;
						return (
							<button
								key={entry.key}
								type="button"
								role="tab"
								aria-selected={on}
								title={stat.title}
								className={on ? "atr-pick atr-pick-on" : "atr-pick"}
								onClick={() => {
									pick(entry.key);
								}}
							>
								<span className="atr-pick-stat">{stat.abbrev}</span>
								<span className="atr-pick-rank">
									{entry.rank === undefined ? "-" : helpers.ordinal(entry.rank)}
								</span>
							</button>
						);
					})}
				</div>
				{current ? (
					<Ladder entry={current} pid={pid} scope={shownScope} />
				) : null}
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
