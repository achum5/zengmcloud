import { useEffect, useMemo, useState } from "react";
import { Modal } from "./Modal.tsx";
import { TeamLogoInline } from "./TeamLogoInline.tsx";
import { TriviaSquares } from "./TriviaSquares.tsx";
import { useLocal } from "../util/local.ts";
import {
	deleteHistoryEntry,
	filterHistory,
	loadHistory,
	type HistorySort,
	type TriviaGame,
	type TriviaHistoryEntry,
	type TriviaReplay,
} from "../util/triviaHistory.ts";
import { loadSharedHistory, shareHistory } from "../util/triviaHistorySync.ts";

// Game History: every finished game in the room, not just yours. Each row says
// who played it, how they did, and - for a grid - the shape of their board, so
// you can see a result without seeing an answer. Rows with replay data load
// that exact game.

const fmtWhen = (ts: number) => {
	const d = new Date(ts);
	return `${d.toLocaleDateString(undefined, {
		month: "short",
		day: "numeric",
		year: "numeric",
	})} at ${d.toLocaleTimeString(undefined, {
		hour: "numeric",
		minute: "2-digit",
	})}`;
};

export const TriviaHistoryModal = ({
	game,
	show,
	onHide,
	onReplay,
}: {
	game: TriviaGame;
	show: boolean;
	onHide: () => void;
	// Load the recorded game. Absent replay data (an old entry) hides the button
	// rather than offering one that does nothing.
	onReplay?: (replay: TriviaReplay) => void;
}) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const [entries, setEntries] = useState<TriviaHistoryEntry[]>(() =>
		loadHistory(game),
	);
	const [showFilter, setShowFilter] = useState(false);
	const [query, setQuery] = useState("");
	const [tid, setTid] = useState<number | undefined>();
	const [sort, setSort] = useState<HistorySort>("recent");

	// Pull the room's results every time the modal opens - someone else may have
	// played since it was last looked at.
	useEffect(() => {
		if (!show) {
			return;
		}
		let stale = false;
		void loadSharedHistory(game).then((all) => {
			if (!stale) {
				setEntries(all);
			}
		});
		return () => {
			stale = true;
		};
	}, [show, game]);

	// Only teams that actually appear - a 30-team dropdown where 27 entries match
	// nothing is worse than no dropdown.
	const teamOptions = useMemo(() => {
		const tids = new Set<number>();
		for (const e of entries) {
			for (const t of [e.tid, e.byTid]) {
				if (t !== undefined && t >= 0) {
					tids.add(t);
				}
			}
		}
		return [...tids]
			.map((t) => ({
				tid: t,
				label:
					`${teamInfoCache[t]?.region ?? ""} ${teamInfoCache[t]?.name ?? t}`.trim(),
			}))
			.sort((a, b) => a.label.localeCompare(b.label));
	}, [entries, teamInfoCache]);

	const shown = useMemo(
		() => filterHistory(entries, { query, tid, sort }),
		[entries, query, tid, sort],
	);

	const remove = (id: string) => {
		const next = deleteHistoryEntry(game, id);
		shareHistory(game, next);
		setEntries((prev) => prev.filter((e) => e.id !== id));
	};

	return (
		<Modal show={show} onHide={onHide} size="lg" scrollable>
			<Modal.Header closeButton>
				<Modal.Title className="fs-5 d-flex align-items-center gap-3">
					Game history
					{entries.length > 0 ? (
						<button
							type="button"
							className={`btn btn-sm ${showFilter ? "btn-primary" : "btn-light-bordered"}`}
							onClick={() => setShowFilter((v) => !v)}
						>
							Filter
						</button>
					) : null}
				</Modal.Title>
			</Modal.Header>
			<Modal.Body>
				{showFilter ? (
					<div className="d-flex flex-wrap gap-2 mb-3">
						<input
							className="form-control form-control-sm w-auto flex-grow-1"
							type="text"
							value={query}
							placeholder="Search"
							autoComplete="off"
							onChange={(event) => setQuery(event.target.value)}
						/>
						{teamOptions.length > 1 ? (
							<select
								className="form-select form-select-sm w-auto"
								value={tid === undefined ? "" : String(tid)}
								onChange={(event) =>
									setTid(
										event.target.value === ""
											? undefined
											: Number(event.target.value),
									)
								}
							>
								<option value="">All teams</option>
								{teamOptions.map((t) => (
									<option key={t.tid} value={t.tid}>
										{t.label}
									</option>
								))}
							</select>
						) : null}
						<select
							className="form-select form-select-sm w-auto"
							value={sort}
							onChange={(event) => setSort(event.target.value as HistorySort)}
						>
							<option value="recent">Most recent</option>
							<option value="best">Highest score</option>
						</select>
					</div>
				) : null}

				{entries.length === 0 ? (
					<p className="text-body-secondary mb-0">
						No games yet. Finish one and it'll show up here.
					</p>
				) : shown.length === 0 ? (
					<p className="text-body-secondary mb-0">Nothing matches that.</p>
				) : (
					<div className="d-flex flex-column gap-2">
						{shown.map((e) => {
							// The logo is whoever PLAYED it - that's the thing you scan for
							// in a room's scoreboard. A roster quiz falls back to its
							// subject team, which is the only team it has.
							const logoTid = e.byTid ?? e.tid;
							const t =
								logoTid === undefined || logoTid < 0
									? undefined
									: teamInfoCache[logoTid];
							const mine = e.byName === undefined;
							return (
								<div key={e.id} className="trivia-history-row">
									{t ? (
										<TeamLogoInline
											imgURL={t.imgURL}
											imgURLSmall={t.imgURLSmall}
											size={32}
											includePlaceholderIfNoLogo
										/>
									) : null}
									{e.cells ? <TriviaSquares cells={e.cells} /> : null}
									<div className="flex-grow-1" style={{ minWidth: 0 }}>
										<div className="fw-bold text-truncate">{e.label}</div>
										<div className="trivia-history-sub text-truncate">
											{mine ? "You" : (e.byName ?? "Someone")}
											{t ? ` · ${t.abbrev}` : ""} · {e.detail} ·{" "}
											{fmtWhen(e.ts)}
										</div>
									</div>
									<div className="text-end flex-shrink-0">
										<div className="trivia-history-score">{e.score}</div>
										<div className="trivia-history-sub">points</div>
									</div>
									{onReplay && e.replay ? (
										<button
											type="button"
											className="btn btn-sm btn-light-bordered flex-shrink-0"
											onClick={() => {
												onReplay(e.replay!);
												onHide();
											}}
										>
											Play
										</button>
									) : null}
									{mine ? (
										<button
											type="button"
											className="trivia-history-delete"
											title="Delete"
											onClick={() => remove(e.id)}
										>
											&times;
										</button>
									) : null}
								</div>
							);
						})}
					</div>
				)}
			</Modal.Body>
		</Modal>
	);
};
