import { DataTable } from "../components/DataTable/index.tsx";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { showNotification } from "../util/showNotification.ts";
import { toWorker } from "../util/toWorker.ts";
import { getCols } from "../../common/getCols.ts";
import type { View } from "../../common/types.ts";
import { wrappedMood } from "../components/Mood.tsx";
import { wrappedPlayerNameLabels } from "../components/PlayerNameLabels.tsx";
import type { DataTableRow } from "../components/DataTable/index.tsx";
import { wrappedCurrency } from "../components/wrappedCurrency.ts";
import { SafeHtml } from "../components/SafeHtml.tsx";
import { NegotiateButtons } from "../components/NegotiateButtons.tsx";
import { RosterComposition } from "../components/RosterComposition.tsx";
import { RosterSalarySummary } from "../components/RosterSalarySummary.tsx";
import { SalaryCapInfo } from "../components/SalaryCapInfo.tsx";
import { confirm } from "../util/confirm.tsx";
import {
	NegotiationModal,
	useNegotiaionModal,
} from "../components/NegotiationModal.tsx";
import { useLocal } from "../util/local.ts";
import clsx from "clsx";
import { showUndoNotification } from "../components/UndoNotification.tsx";
import { useEffect, useRef, useState } from "react";
import type api from "../../worker/api/index.ts";
import {
	AmountStepper,
	amountForYears,
	formatChance,
	formatOfferAmount,
	useRealisticFrontOffices,
} from "../components/OfferEditor.tsx";

const showReleaseUndo = ({
	name,
	undoKey,
}: {
	name: string;
	undoKey: number;
}) => {
	showUndoNotification({
		actionName: "release",
		undoKey,
		title: `You released ${name}`,
	});
};

type ReSignOffer = NonNullable<
	View<"negotiationList">["players"][number]["reSignOffer"]
>;

type Edit = { amount: number; years: number; chance: number | undefined };

// Realistic front offices: the re-signing offers as the user changes them -
// the odds follow at once, and each offer is saved a moment after the last
// change.
const useReSignEdits = (
	players: { pid: number; reSignOffer?: ReSignOffer }[],
) => {
	const [edits, setEdits] = useState<Record<number, Edit>>({});
	const requests = useRef(new Map<number, number>());
	const chanceTimers = useRef(new Map<number, number>());
	const saves = useRef(
		new Map<number, { timer: number; run: () => void; started?: boolean }>(),
	);

	const drop = (pid: number) => {
		requests.current.set(pid, (requests.current.get(pid) ?? 0) + 1);
		clearTimeout(chanceTimers.current.get(pid));
		const save = saves.current.get(pid);
		if (save) {
			clearTimeout(save.timer);
			saves.current.delete(pid);
		}
		setEdits((prev) => {
			const next = { ...prev };
			delete next[pid];
			return next;
		});
	};

	const change = (
		pid: number,
		offer: ReSignOffer,
		edit: Omit<Edit, "chance">,
	) => {
		const term = offer.terms.find((term) => term.years === edit.years);
		if (!term) {
			return;
		}
		const id = (requests.current.get(pid) ?? 0) + 1;
		requests.current.set(pid, id);
		setEdits((prev) => ({ ...prev, [pid]: { ...edit, chance: undefined } }));

		clearTimeout(chanceTimers.current.get(pid));
		chanceTimers.current.set(
			pid,
			window.setTimeout(async () => {
				const chance = await toWorker("main", "getOfferChance", {
					pid,
					amount: edit.amount,
					exp: term.exp,
				});
				if (requests.current.get(pid) === id) {
					setEdits((prev) =>
						prev[pid] ? { ...prev, [pid]: { ...prev[pid], chance } } : prev,
					);
				}
			}, 150),
		);

		const prevSave = saves.current.get(pid);
		if (prevSave) {
			clearTimeout(prevSave.timer);
		}
		const run = async () => {
			const entry = saves.current.get(pid);
			if (entry?.run === run) {
				entry.started = true;
			}
			const error = await toWorker("main", "makeFreeAgentOffer", {
				pid,
				amount: edit.amount,
				exp: term.exp,
			});
			if (saves.current.get(pid)?.run === run) {
				saves.current.delete(pid);
			}
			if (typeof error === "string") {
				showNotification({ type: "error", text: error });
				if (requests.current.get(pid) === id) {
					drop(pid);
				}
			}
		};
		saves.current.set(pid, { timer: window.setTimeout(run, 600), run });
	};

	// Leaving the page saves what is waiting to be saved.
	useEffect(() => {
		const pending = saves.current;
		return () => {
			for (const save of pending.values()) {
				clearTimeout(save.timer);
				if (!save.started) {
					save.run();
				}
			}
		};
	}, []);

	// Once the league has an offer as edited, show the league's.
	const serverKey = JSON.stringify(
		players.map((p) => [p.pid, p.reSignOffer?.offer]),
	);
	useEffect(() => {
		setEdits((prev) => {
			let next = prev;
			for (const p of players) {
				const edit = prev[p.pid];
				const offer = p.reSignOffer?.offer;
				if (
					edit &&
					!saves.current.has(p.pid) &&
					(!offer ||
						(offer.amount === edit.amount && offer.years === edit.years))
				) {
					if (next === prev) {
						next = { ...prev };
					}
					delete next[p.pid];
				}
			}
			return next;
		});
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [serverKey]);

	return { change, drop, edits };
};

const ReSignOfferCell = ({
	disabled,
	edit,
	offer,
	onChange,
}: {
	disabled: boolean;
	edit: Edit | undefined;
	offer: ReSignOffer;
	onChange: (edit: Omit<Edit, "chance">) => void;
}) => {
	const { maxContract, minContract } = useLocal(["maxContract", "minContract"]);
	const current = edit ?? offer.offer;
	if (!current) {
		return <span className="text-body-secondary">Let go</span>;
	}
	const term =
		offer.terms.find((term) => term.years === current.years) ?? offer.terms[0]!;
	return (
		<div className="d-flex align-items-center gap-1">
			<AmountStepper
				amount={current.amount}
				ask={term.ask}
				disabled={disabled}
				maxContract={maxContract}
				minContract={minContract}
				onChange={(amount) => onChange({ amount, years: current.years })}
				size="sm"
			/>
			<select
				className="form-select form-select-sm"
				style={{ width: "auto" }}
				disabled={disabled}
				value={current.years}
				onChange={(event) => {
					const years = Number.parseInt(event.target.value);
					onChange({
						amount: amountForYears(
							offer.terms,
							current.amount,
							current.years,
							years,
						),
						years,
					});
				}}
				title={`Asking ${formatOfferAmount(term.ask)} thru ${term.exp}`}
			>
				{offer.terms.map((option) => (
					<option key={option.years} value={option.years}>
						{option.years} {helpers.plural("yr", option.years)}
					</option>
				))}
			</select>
		</div>
	);
};

// Realistic front offices: who stayed and who is testing the market.
const DecidedPanel = ({
	decided,
	onClose,
}: {
	decided: {
		pid: number;
		name: string;
		chance: number;
		signed: boolean;
		amount: number;
		exp: number;
	}[];
	onClose: () => void;
}) => {
	const stayed = decided.filter((row) => row.signed);
	const left = decided.filter((row) => !row.signed);
	const link = (row: (typeof decided)[number]) => (
		<a href={helpers.leagueUrl(["player", row.pid])}>{row.name}</a>
	);
	return (
		<div>
			<div
				className="alert alert-secondary d-inline-block position-relative pe-5"
				data-testid="re-sign-decided"
			>
				<button
					type="button"
					className="btn-close position-absolute"
					style={{ top: 12, right: 12 }}
					aria-label="Close"
					onClick={onClose}
				/>
				{stayed.length > 0 ? (
					<div>
						<b>Stayed:</b>{" "}
						{stayed.map((row, i) => (
							<span key={row.pid}>
								{i > 0 ? ", " : null}
								{link(row)} ({formatOfferAmount(row.amount)} thru {row.exp},{" "}
								{formatChance(row.chance)})
							</span>
						))}
					</div>
				) : null}
				{left.length > 0 ? (
					<div>
						<b>Left:</b>{" "}
						{left.map((row, i) => (
							<span key={row.pid}>
								{i > 0 ? ", " : null}
								{link(row)} ({formatChance(row.chance)})
							</span>
						))}
					</div>
				) : null}
			</div>
		</div>
	);
};

const NegotiationList = ({
	capSpace,
	draftPickAutoContract,
	numRosterSpots,
	payroll,
	players,
	stats,
	sumContracts,
	userPlayers,
}: View<"negotiationList">) => {
	const {
		challengeNoRatings,
		gameSimInProgress,
		minContract,
		salaryCapType,
		spectator,
		season,
	} = useLocal([
		"challengeNoRatings",
		"gameSimInProgress",
		"minContract",
		"salaryCapType",
		"spectator",
		"season",
	]);

	const title =
		salaryCapType === "hard" || !draftPickAutoContract
			? "Rookies and Expiring Contracts"
			: "Re-sign Players";

	useTitleBar({ title });

	const negotiationModal = useNegotiaionModal();

	// Realistic front offices: each player has an offer and his chance of
	// taking it, instead of contracts to sign.
	const realistic = useRealisticFrontOffices();
	const { change, drop, edits } = useReSignEdits(players);
	const [decided, setDecided] =
		useState<
			Exclude<
				Awaited<ReturnType<typeof api.main.decideReSigningOffers>>,
				string
			>
		>();

	if (spectator) {
		return <p>The AI will handle re-signing players in spectator mode.</p>;
	}

	const cols = getCols(
		[
			"Name",
			"Pos",
			"Age",
			"Ovr",
			"Pot",
			...stats.map((stat) => `stat:${stat}`),
			"Acquired",
			"Mood",
			"Old Salary",
			...(realistic ? ["Offer", "Odds"] : ["Asking For", "Exp"]),
			"Actions",
		],
		{
			Actions: {
				width: "1px",
			},
			Odds: {
				desc: "Chance he takes your offer",
			},
		},
	);

	const offerOf = (pid: number, offer: ReSignOffer) => {
		const edit = edits[pid];
		return edit ?? offer.offer;
	};

	const offerCells = (p: (typeof players)[number]) => {
		if (!p.reSignOffer) {
			// A draft pick, who signs as in stock.
			return [
				{
					value: (
						<>
							{helpers.formatCurrency(p.mood.user.contractAmount / 1000, "M")}{" "}
							thru {p.contract.exp}
						</>
					),
					sortValue: p.mood.user.contractAmount,
				},
				null,
			];
		}
		const offer = p.reSignOffer;
		const current = offerOf(p.pid, offer);
		const edit = edits[p.pid];
		const chance = edit ? edit.chance : offer.chance;
		return [
			{
				value: (
					<ReSignOfferCell
						disabled={gameSimInProgress}
						edit={edit}
						offer={offer}
						onChange={(next) => change(p.pid, offer, next)}
					/>
				),
				sortValue: current?.amount ?? -1,
				searchValue: current ? formatOfferAmount(current.amount) : "Let go",
			},
			{
				value: current ? (
					<span className="fw-bold">{formatChance(chance)}</span>
				) : null,
				sortValue: current ? (chance ?? 0) : -1,
			},
		];
	};

	const offerActions = (pid: number, offer: ReSignOffer) => {
		const current = offerOf(pid, offer);
		return {
			value: current ? (
				<button
					type="button"
					className="btn btn-light-bordered btn-xs"
					disabled={gameSimInProgress}
					onClick={async () => {
						drop(pid);
						await toWorker("main", "withdrawFreeAgentOffer", pid);
					}}
					title="Withdraw your offer"
				>
					Let go
				</button>
			) : (
				<button
					type="button"
					className="btn btn-light-bordered btn-xs"
					disabled={gameSimInProgress}
					onClick={() => {
						const term = offer.terms.find(
							(term) => term.years === offer.wantedYears,
						)!;
						change(pid, offer, { amount: term.ask, years: term.years });
					}}
				>
					Offer
				</button>
			),
			searchValue: current ? "Let go" : "Offer",
		};
	};

	const rows: DataTableRow<"player">[] = players.map((p) => {
		const negotiateButtons = (
			<NegotiateButtons
				canGoOverCap={salaryCapType === "none" || salaryCapType === "soft"}
				capSpace={capSpace}
				minContract={minContract}
				onNegotiate={async () => {
					await negotiationModal.negotiate(p.pid);
				}}
				spectator={spectator}
				p={p}
				willingToNegotiate={p.mood.user.willing}
			/>
		);

		return {
			key: p.pid,
			metadata: {
				type: "player",
				pid: p.pid,
				season,
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
				}),
				p.ratings.pos,
				p.age,
				!challengeNoRatings ? p.ratings.ovr : null,
				!challengeNoRatings ? p.ratings.pot : null,
				...stats.map((stat) => helpers.roundStat(p.stats[stat], stat)),
				{
					value: <SafeHtml dirty={p.latestTransaction} />,
					searchValue: p.latestTransaction,
					sortValue: p.latestTransactionSeason,
				},
				wrappedMood({
					defaultType: "user",
					maxWidth: true,
					p,
				}),
				// Players who have never been paid have no previous salary
				p.lastSalary === undefined
					? {
							value: null,
							classNames: "text-body-secondary",
						}
					: {
							...wrappedCurrency(p.lastSalary, "M"),
							classNames: "text-body-secondary",
						},
				...(realistic
					? offerCells(p)
					: [
							wrappedCurrency(p.mood.user.contractAmount / 1000, "M"),
							p.contract.exp,
						]),
				realistic && p.reSignOffer
					? offerActions(p.pid, p.reSignOffer)
					: {
							value: (
								<>
									{negotiateButtons}
									<button
										type="button"
										className={clsx(
											"btn btn-light-bordered btn-xs",
											p.mood.user.willing ? undefined : "ms-auto",
										)}
										onClick={async () => {
											const undoKey = await toWorker(
												"main",
												"cancelContractNegotiation",
												p.pid,
											);
											showReleaseUndo({
												name: `${p.firstName} ${p.lastName}`,
												undoKey,
											});
										}}
									>
										Release
									</button>
								</>
							),
							classNames: "d-flex align-items-center gap-2",
							searchValue: p.mood.user.willing
								? "Negotiate Sign Release"
								: "Refuses! Release",
						},
			],
			classNames: {
				"table-info": p.contract.rookie,
			},
		};
	});

	const hasRookies = players.some((p) => p.contract.rookie);

	let offersTotal = 0;
	let numOffers = 0;
	for (const p of players) {
		const current = p.reSignOffer ? offerOf(p.pid, p.reSignOffer) : undefined;
		if (current) {
			offersTotal += current.amount;
			numOffers += 1;
		}
	}

	return (
		<>
			<RosterComposition className="float-end mb-3" players={userPlayers} />

			<p>
				More:{" "}
				<a href={helpers.leagueUrl(["upcoming_free_agents"])}>
					Upcoming Free Agents
				</a>
			</p>

			{salaryCapType === "soft" ? (
				<p>
					You are allowed to go over the salary cap to re-sign your players
					before they become free agents. If you do not re-sign them before free
					agency begins, they will be free to sign with any team, and you won't
					be able to go over the salary cap to sign them.
				</p>
			) : null}

			<RosterSalarySummary
				capSpace={capSpace}
				numRosterSpots={numRosterSpots}
				payroll={payroll}
			/>

			<SalaryCapInfo className="mb-3" />

			<p>
				{realistic ? (
					<>
						Your offers total <b>{formatOfferAmount(offersTotal)}</b>.
					</>
				) : (
					<>
						Your unsigned players are asking for a total of{" "}
						<b>{helpers.formatCurrency(sumContracts, "M")}</b>.
					</>
				)}
				{hasRookies ? (
					<>
						{" "}
						Rookies you just drafted are{" "}
						<span className="text-info">highlighted in blue</span>.
					</>
				) : null}
			</p>

			{decided && decided.length > 0 ? (
				<DecidedPanel decided={decided} onClose={() => setDecided(undefined)} />
			) : null}

			{realistic ? (
				<button
					className="btn btn-secondary mb-3"
					disabled={numOffers === 0 || gameSimInProgress}
					onClick={async () => {
						const proceed = await confirm(
							`Decide all ${numOffers} ${helpers.plural("offer", numOffers)} now?`,
							{
								okText: "Decide",
							},
						);
						if (!proceed) {
							return;
						}
						const response = await toWorker(
							"main",
							"decideReSigningOffers",
							undefined,
						);
						if (typeof response === "string") {
							showNotification({
								type: "error",
								text: response,
							});
						} else {
							setDecided(response);
						}
					}}
				>
					Decide all now
				</button>
			) : (salaryCapType !== "hard" || sumContracts < capSpace) &&
			  players.length > 0 ? (
				<button
					className="btn btn-secondary mb-3"
					onClick={async () => {
						const proceed = await confirm(
							`Are you sure you want to re-sign all ${
								players.length
							} ${helpers.plural("player", players.length)}?`,
							{
								okText: "Re-sign all",
							},
						);
						if (!proceed) {
							return;
						}

						const response = await toWorker("main", "reSignAll", players);

						if (typeof response === "string") {
							showNotification({
								type: "error",
								text: response,
							});
						} else {
							showUndoNotification({
								actionName: "signings",
								undoKey: response,
								title: `Re-signed all ${
									players.length
								} ${helpers.plural("player", players.length)}`,
							});
						}
					}}
				>
					Re-sign all
				</button>
			) : null}

			<DataTable
				cols={cols}
				defaultSort={[10, "desc"]}
				name="NegotiationList"
				rows={rows}
			/>

			<NegotiationModal {...negotiationModal.props} />
		</>
	);
};

export default NegotiationList;
