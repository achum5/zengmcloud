import { useState } from "react";
import { PHASE, PHASE_TEXT } from "../../common/constants.ts";
import { DataTable } from "../components/DataTable/index.tsx";
import { MoreLinks } from "../components/MoreLinks.tsx";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { toWorker } from "../util/toWorker.ts";
import { getCols } from "../../common/getCols.ts";
import { useLocal } from "../util/local.ts";
import type { Phase, View } from "../../common/types.ts";
import { wrappedMood } from "../components/Mood.tsx";
import {
	wrappedContractAmount,
	wrappedContractExp,
} from "../components/contract.tsx";
import { wrappedPlayerNameLabels } from "../components/PlayerNameLabels.tsx";
import { range } from "../../common/utils.ts";
import type { DropdownOption } from "../hooks/useDropdownOptions.tsx";
import type { FreeAgentTransaction } from "../../worker/views/freeAgents.ts";
import {
	type DataTableHandle,
	type DataTableRow,
} from "../components/DataTable/index.tsx";
import { NegotiateButtons } from "../components/NegotiateButtons.tsx";
import { RosterComposition } from "../components/RosterComposition.tsx";
import { RosterSalarySummary } from "../components/RosterSalarySummary.tsx";
import {
	NegotiationModal,
	useNegotiaionModal,
} from "../components/NegotiationModal.tsx";

const useSeasonsFreeAgents = () => {
	const { phase, season, startingSeason } = useLocal([
		"phase",
		"season",
		"startingSeason",
	]);

	// Decrease season by 1, since "free agent season" starts in the previous calendar year
	const minFreeAgencySeason = startingSeason - 1;

	// These are 1 lower than you'd expect, because there's also a "current" entry added below
	const maxFreeAgencySeason = phase >= PHASE.PLAYOFFS ? season - 1 : season - 2;

	const options: DropdownOption[] = range(
		minFreeAgencySeason,
		maxFreeAgencySeason + 1,
	).map((freeAgencySeason) => {
		let value;
		if (freeAgencySeason >= -10 && freeAgencySeason < 10) {
			value = `${freeAgencySeason}-${freeAgencySeason + 1}`;
		} else {
			value = `${freeAgencySeason}-${String((freeAgencySeason + 1) % 100).padStart(2)}`;
		}

		return {
			key: freeAgencySeason,
			value,
		};
	});

	options.push({
		key: "current",
		value: "Current",
	});

	options.reverse();

	return options;
};

const signedFreeAgentWrapped = (
	freeAgentTransaction: FreeAgentTransaction & {
		abbrev: string;
	},
	freeAgencySeason: number,
	season: number | "current",
	phase: Phase,
) => {
	let rosterSeason;

	if (season === "current" && phase >= PHASE.PLAYOFFS) {
		// Link to current season roster, because there is no next season roster
		rosterSeason = freeAgencySeason;
	} else {
		// Link to next season roster, because freeAgencySeason starts after the regular season ends
		rosterSeason = freeAgencySeason + 1;
	}

	return {
		value: (
			<>
				<a
					href={helpers.leagueUrl([
						"roster",
						`${freeAgentTransaction.abbrev}_${freeAgentTransaction.tid}`,
						rosterSeason,
					])}
				>
					{freeAgentTransaction.abbrev}
				</a>
				, {PHASE_TEXT[freeAgentTransaction.phase]}
			</>
		),
		searchValue: `${freeAgentTransaction.abbrev}, ${PHASE_TEXT[freeAgentTransaction.phase]}`,
	};
};

// One resolved item in the FA day results panel. Contested signings show the
// full roll: every team's mood, odds, and 1-100 band, plus where it landed.
const FaResultItem = ({ item }: { item: FaDayResultItemType }) => {
	if (item.type === "refused") {
		return (
			<div className="text-body-secondary">
				{item.name} refused to negotiate with {item.abbrev}
			</div>
		);
	}
	if (item.type === "ineligible") {
		return (
			<div className="text-body-secondary">
				{item.abbrev} couldn't afford {item.name}
			</div>
		);
	}
	if (item.type === "unopposed") {
		return (
			<div>
				<a href={helpers.leagueUrl(["player", item.pid])}>{item.name}</a> →{" "}
				{item.abbrev} unopposed,{" "}
				{helpers.formatCurrency(item.amount / 1000, "M")} thru {item.exp}
			</div>
		);
	}
	return (
		<div className="mb-2">
			<div>
				<a href={helpers.leagueUrl(["player", item.pid])}>{item.name}</a> →{" "}
				<b>
					{item.teams.find((t) => t.tid === item.winnerTid)?.abbrev ?? "???"}
				</b>
				, {helpers.formatCurrency(item.amount / 1000, "M")} thru {item.exp} ·
				rolled <b>{item.roll}</b>
			</div>
			{item.teams.map((t) => (
				<div
					key={t.tid}
					className={
						t.tid === item.winnerTid ? "text-success" : "text-body-secondary"
					}
				>
					{t.abbrev} · mood {t.mood >= 0 ? "+" : ""}
					{t.mood} · {t.oddsPct}% · {t.lo}–{t.hi}
					{t.tid === item.winnerTid ? " ✓" : ""}
				</div>
			))}
		</div>
	);
};

type FaBoardProps = NonNullable<View<"freeAgents">["faBoard"]>;
type FaDayResultItemType = NonNullable<
	FaBoardProps["results"]
>["items"][number];

// The team's ranked free-agent board plus the last day's transparent results.
const FaBoardPanel = ({
	board,
	faBoard,
	players,
	canAfford,
	onMove,
	onRemove,
}: {
	board: number[];
	faBoard: FaBoardProps;
	players: any[];
	canAfford: (p: any) => boolean;
	onMove: (pid: number, dir: -1 | 1) => void;
	onRemove: (pid: number) => void;
}) => {
	const playerByPid = new Map(players.map((p) => [p.pid, p]));
	const results = faBoard.results;

	return (
		<div className="row mb-3">
			<div className="col-md-6 mb-3 mb-md-0">
				<h3>Board</h3>
				{board.length === 0 ? (
					<p className="text-body-secondary mb-0">
						Rank up to {faBoard.numSlots} free agents. Boards resolve when
						everyone readies up for the next day.
					</p>
				) : (
					<ul className="list-unstyled mb-0">
						{board.map((pid, i) => {
							const p = playerByPid.get(pid);
							return (
								<li key={pid} className="d-flex align-items-center gap-2 mb-1">
									<span
										className="text-body-secondary text-end"
										style={{ width: 22 }}
									>
										{i + 1}.
									</span>
									<div className="btn-group">
										<button
											type="button"
											className="btn btn-xs btn-light-bordered"
											disabled={i === 0}
											onClick={() => onMove(pid, -1)}
											title="Move up"
										>
											▲
										</button>
										<button
											type="button"
											className="btn btn-xs btn-light-bordered"
											disabled={i === board.length - 1}
											onClick={() => onMove(pid, 1)}
											title="Move down"
										>
											▼
										</button>
									</div>
									{p ? (
										<a href={helpers.leagueUrl(["player", pid])}>
											{p.firstName} {p.lastName}
										</a>
									) : (
										<span className="text-body-secondary">Signed/gone</span>
									)}
									{p ? (
										<span className="text-body-secondary">
											{helpers.formatCurrency(p.contract.amount, "M")}
										</span>
									) : null}
									{p && !p.mood?.user?.willing ? (
										<span className="badge text-bg-danger">Won't sign</span>
									) : p && !canAfford(p) ? (
										<span className="badge text-bg-danger">Can't afford</span>
									) : null}
									<button
										type="button"
										className="btn btn-xs btn-light-bordered"
										onClick={() => onRemove(pid)}
										title="Remove"
									>
										✕
									</button>
								</li>
							);
						})}
					</ul>
				)}
			</div>
			{results ? (
				<div className="col-md-6">
					<h3>
						Last day results{" "}
						<span className="text-body-secondary fs-6">
							({results.daysLeft} days left)
						</span>
					</h3>
					{results.items.length === 0 ? (
						<p className="text-body-secondary mb-0">No board signings.</p>
					) : (
						results.items.map((item, i) => <FaResultItem key={i} item={item} />)
					)}
					<details className="mt-2">
						<summary className="text-body-secondary">Boards</summary>
						{results.boards.map((b) => (
							<div key={b.tid}>
								<b>{b.abbrev}:</b>{" "}
								{b.pids.map((row) => row.name).join(", ") || "—"}
							</div>
						))}
					</details>
				</div>
			) : null}
		</div>
	);
};

const FreeAgents = ({
	capSpace,
	challengeNoFreeAgents,
	faBoard,
	freeAgencySeason,
	numRosterSpots,
	payroll,
	players,
	season,
	stats,
	type,
	userPlayers,
}: View<"freeAgents">) => {
	const seasonsFreeAgents = useSeasonsFreeAgents();
	const [board, setBoard] = useState<number[]>(faBoard?.pids ?? []);
	const [hideRefusals, setHideRefusals] = useState(false);

	// Every board edit publishes to the room (fire-and-forget); the resolution
	// reads whatever the room has when the day advances.
	const updateBoard = (next: number[]) => {
		setBoard(next);
		void toWorker("main", "faBoardSet", next);
	};

	// Same affordability rule as the Sign button: under the cap (or it's a
	// min contract, or the league has no cap). Amounts here are in millions.
	const canAffordBoard = (p: any) =>
		salaryCapType === "none" ||
		p.contract.amount <= capSpace + 1 / 1000 ||
		p.contract.amount <= (minContract + 1) / 1000;

	useTitleBar({
		title: "Free Agents",
		dropdownView: "free_agents",
		dropdownFields: { typeFreeAgents: type, seasonsFreeAgents: season },
		dropdownCustomOptions: {
			seasonsFreeAgents,
		},
	});

	const {
		challengeNoRatings,
		gameSimInProgress,
		minContract,
		phase,
		salaryCapType,
		spectator,
	} = useLocal([
		"challengeNoRatings",
		"gameSimInProgress",
		"minContract",
		"phase",
		"salaryCapType",
		"spectator",
	]);

	const [dataTableHandle, setDataTableHandle] =
		useState<DataTableHandle | null>(null);

	const negotiationModal = useNegotiaionModal();

	if (
		((phase > PHASE.AFTER_TRADE_DEADLINE && phase <= PHASE.RESIGN_PLAYERS) ||
			phase === PHASE.FANTASY_DRAFT ||
			phase === PHASE.EXPANSION_DRAFT) &&
		season === "current"
	) {
		return (
			<div>
				<MoreLinks type="freeAgents" page="free_agents" />
				<p>You're not allowed to sign free agents now.</p>
				<p>
					Free agents can only be signed before the playoffs or after players
					are re-signed.
				</p>
			</div>
		);
	}

	const askingForText = "Asking For";
	const colKeys = [
		"Name",
		"Pos",
		"Age",
		"Ovr",
		"Pot",
		...stats.map((stat) => `stat:${stat}`),
		"Mood",
		askingForText,
		"Exp",
		"Actions",
	];
	const cols = getCols(colKeys, {
		Actions: {
			width: "1px",
		},
	});

	const showShowPlayersAffordButton = salaryCapType !== "none";

	// These are used in showAffordablePlayersFilterApplied calculation every render, and then also in toggleShowAfforablePlayers when that is called
	let askingForIndex = -1;
	let askingForFilter = "";

	let showAffordablePlayersFilterApplied = false;
	if (showShowPlayersAffordButton && dataTableHandle) {
		askingForIndex = colKeys.lastIndexOf(askingForText);
		if (capSpace * 1000 > minContract && !challengeNoFreeAgents) {
			askingForFilter = `<${capSpace}`;
		} else {
			askingForFilter = `<${minContract / 1000}`;
		}

		const enableFilters = dataTableHandle.getEnableFilters();
		if (enableFilters) {
			const filters = dataTableHandle.getFilters();
			showAffordablePlayersFilterApplied =
				filters[askingForIndex] === askingForFilter;
		}
	}

	const toggleShowAfforablePlayers = () => {
		if (dataTableHandle) {
			const enableFilters = dataTableHandle.getEnableFilters();

			// Start from either the current filters (if they are shown/enabled) or no filters at all
			const filters: string[] = enableFilters
				? [...dataTableHandle.getFilters()]
				: new Array(cols.length).fill("");

			// If we currently have this exact filter set, delete it. Otherwise, add it
			let newEnableFilters = true;
			if (filters[askingForIndex] === askingForFilter) {
				filters[askingForIndex] = "";

				// If no other filters are applied, hide filter bar
				if (filters.every((filter) => filter === "")) {
					newEnableFilters = false;
				}
			} else {
				filters[askingForIndex] = askingForFilter;
			}

			dataTableHandle.setFilters(filters, newEnableFilters);
		}
	};

	const playerInfoSeason =
		freeAgencySeason +
		(season === "current" && phase < PHASE.FREE_AGENCY ? 1 : 0);

	// "Hide refusals" filter: drop available free agents unwilling to sign with
	// you (signed/historical rows have no refusal concept and always show).
	const shownPlayers = hideRefusals
		? players.filter(
				(p) => p.freeAgentType !== "available" || p.mood?.user?.willing,
			)
		: players;

	const rows: DataTableRow[] = shownPlayers.map((p) => {
		return {
			key: p.pid,
			metadata: {
				type: "player",
				pid: p.pid,
				season: playerInfoSeason,
				playoffs: "regularSeason",
			},
			data: [
				wrappedPlayerNameLabels({
					pid: p.pid,
					injury: p.injury,
					jerseyNumber: p.jerseyNumber,
					skills: p.ratings.skills,
					defaultWatch: p.watch,
					firstName: p.firstName,
					firstNameShort: p.firstNameShort,
					lastName: p.lastName,
					season: playerInfoSeason,
				}),
				p.ratings.pos,
				p.age,
				!challengeNoRatings ? p.ratings.ovr : null,
				!challengeNoRatings ? p.ratings.pot : null,
				...stats.map((stat) => helpers.roundStat(p.stats[stat], stat)),
				p.freeAgentType === "available"
					? wrappedMood({
							defaultType: "user",
							maxWidth: true,
							p,
						})
					: undefined,
				wrappedContractAmount(p, p.contract.amount),
				wrappedContractExp(p),
				p.freeAgentType === "available"
					? faBoard
						? {
								value: board.includes(p.pid) ? (
									<button
										type="button"
										className="btn btn-sm btn-secondary"
										onClick={() =>
											updateBoard(board.filter((pid) => pid !== p.pid))
										}
									>
										#{board.indexOf(p.pid) + 1} ✕
									</button>
								) : (
									<button
										type="button"
										className="btn btn-sm btn-light-bordered"
										disabled={
											board.length >= faBoard.numSlots ||
											!p.mood.user.willing ||
											!canAffordBoard(p)
										}
										title={
											!p.mood.user.willing
												? "Refuses to sign with you"
												: !canAffordBoard(p)
													? "Can't afford"
													: undefined
										}
										onClick={() => updateBoard([...board, p.pid])}
									>
										Board
									</button>
								),
								searchValue: "Board",
							}
						: {
								value: (
									<NegotiateButtons
										canGoOverCap={salaryCapType === "none"}
										capSpace={capSpace}
										disabled={gameSimInProgress}
										minContract={minContract}
										onNegotiate={async () => {
											await negotiationModal.negotiate(p.pid);
										}}
										spectator={spectator}
										p={p}
										willingToNegotiate={p.mood.user.willing}
									/>
								),
								classNames: "d-flex align-items-center gap-2",
								searchValue: p.mood.user.willing
									? "Negotiate Sign"
									: "Refuses!",
							}
					: signedFreeAgentWrapped(
							p.freeAgentTransaction,
							freeAgencySeason,
							season,
							phase,
						),
			],
		};
	});

	return (
		<>
			{season === "current" ? (
				<RosterComposition className="float-end mb-3" players={userPlayers} />
			) : null}
			<MoreLinks type="freeAgents" page="free_agents" />
			{season === "current" ? (
				<>
					<RosterSalarySummary
						capSpace={capSpace}
						numRosterSpots={numRosterSpots}
						payroll={payroll}
					/>

					{showShowPlayersAffordButton ? (
						<button
							className="btn btn-secondary mb-3 me-2"
							onClick={toggleShowAfforablePlayers}
						>
							{showAffordablePlayersFilterApplied
								? "Show players with any asking price"
								: "Show players you can afford now"}
						</button>
					) : null}
					<button
						className="btn btn-secondary mb-3"
						onClick={() => setHideRefusals((prev) => !prev)}
					>
						{hideRefusals
							? "Show players who refuse to sign"
							: "Hide players who refuse to sign"}
					</button>
				</>
			) : null}

			{faBoard ? (
				<FaBoardPanel
					board={board}
					faBoard={faBoard}
					players={players}
					canAfford={canAffordBoard}
					onMove={(pid, dir) => {
						const i = board.indexOf(pid);
						const j = i + dir;
						if (i === -1 || j < 0 || j >= board.length) {
							return;
						}
						const next = [...board];
						[next[i], next[j]] = [next[j]!, next[i]!];
						updateBoard(next);
					}}
					onRemove={(pid) => {
						updateBoard(board.filter((pid2) => pid2 !== pid));
					}}
				/>
			) : null}

			{gameSimInProgress && !spectator ? (
				<p className="text-danger">Stop game simulation to sign free agents.</p>
			) : null}

			{spectator ? (
				<div>
					<div className="alert alert-danger d-inline-block">
						The AI will handle signing free agents in spectator mode.
					</div>
				</div>
			) : challengeNoFreeAgents ? (
				<div>
					<div className="alert alert-danger d-inline-block">
						<b>Challenge Mode:</b> You are not allowed to sign free agents,
						except to minimum contracts.
					</div>
				</div>
			) : null}

			<DataTable
				cols={cols}
				defaultSort={[cols.length - 3, "desc"]}
				defaultStickyCols={window.mobile ? 0 : 1}
				name="FreeAgents"
				pagination
				ref={setDataTableHandle}
				rows={rows}
			/>

			<NegotiationModal {...negotiationModal.props} />
		</>
	);
};

export default FreeAgents;
