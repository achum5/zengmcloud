import { useMemo, useState } from "react";
import { Modal } from "./Modal.tsx";
import { TeamLogoInline } from "./TeamLogoInline.tsx";
import { useLocal } from "../util/local.ts";
import {
	deleteHistoryEntry,
	filterHistory,
	type HistorySort,
	type TriviaGame,
	type TriviaHistoryEntry,
} from "../util/triviaHistory.ts";

// The Game History screen shared by the trivia games: every finished game as a
// card in the team's own colors, newest first, with a filter and a per-entry
// delete.

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

// Readable text on a team's primary color. Relative luminance rather than a
// plain average: the eye is far more sensitive to green than to blue, and an
// average makes a saturated blue look lighter than it reads.
export const contrastText = (hex: string | undefined): string => {
	if (!hex || !/^#[\da-f]{6}$/i.test(hex)) {
		return "#fff";
	}
	const channel = (i: number) => {
		const v = Number.parseInt(hex.slice(1 + i * 2, 3 + i * 2), 16) / 255;
		return v <= 0.040_45 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4;
	};
	const luminance =
		0.2126 * channel(0) + 0.7152 * channel(1) + 0.0722 * channel(2);
	return luminance > 0.45 ? "#000" : "#fff";
};

export const TriviaHistoryModal = ({
	game,
	show,
	onHide,
	entries,
	onChange,
}: {
	game: TriviaGame;
	show: boolean;
	onHide: () => void;
	entries: TriviaHistoryEntry[];
	onChange: (entries: TriviaHistoryEntry[]) => void;
}) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const [showFilter, setShowFilter] = useState(false);
	const [query, setQuery] = useState("");
	const [tid, setTid] = useState<number | undefined>();
	const [sort, setSort] = useState<HistorySort>("recent");

	// Only teams that actually appear in the history - a 30-team dropdown where
	// 27 entries match nothing is worse than no dropdown.
	const teamOptions = useMemo(() => {
		const tids = new Set<number>();
		for (const e of entries) {
			if (e.tid !== undefined) {
				tids.add(e.tid);
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
		onChange(deleteHistoryEntry(game, id));
	};

	return (
		<Modal show={show} onHide={onHide} size="lg">
			<Modal.Header closeButton>
				<Modal.Title className="fs-5 d-flex align-items-center gap-2">
					<span aria-hidden="true">🕘</span> Game History
					{entries.length > 0 ? (
						<button
							type="button"
							className={`btn btn-sm ${showFilter ? "btn-primary" : "btn-light-bordered"}`}
							title="Filter"
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
							placeholder="Search…"
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
							const t = e.tid === undefined ? undefined : teamInfoCache[e.tid];
							const bg = e.colors?.[0];
							const fg = contrastText(bg);
							return (
								<div
									key={e.id}
									className="trivia-history-row"
									style={
										bg
											? {
													background: bg,
													borderColor: e.colors?.[1] ?? bg,
													color: fg,
												}
											: undefined
									}
								>
									{t ? (
										<TeamLogoInline
											imgURL={t.imgURL}
											imgURLSmall={t.imgURLSmall}
											size={36}
											includePlaceholderIfNoLogo
										/>
									) : null}
									<div className="flex-grow-1" style={{ minWidth: 0 }}>
										<div className="fw-bold text-truncate">{e.label}</div>
										<div className="trivia-history-sub text-truncate">
											{e.detail ? `${e.detail} · ` : ""}
											{fmtWhen(e.ts)}
										</div>
									</div>
									<div className="text-end flex-shrink-0">
										<div className="trivia-history-score">{e.score}</div>
										<div className="trivia-history-sub">points</div>
									</div>
									<button
										type="button"
										className="trivia-history-delete"
										title="Delete this game"
										style={bg ? { color: fg } : undefined}
										onClick={() => remove(e.id)}
									>
										✕
									</button>
								</div>
							);
						})}
					</div>
				)}
			</Modal.Body>
		</Modal>
	);
};
