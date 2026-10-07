import { PHASE, PLAYER } from "../../../common/constants.ts";
import { idb } from "../../db/index.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { g, helpers, lock, toUI, updatePlayMenu } from "../../util/index.ts";
import type {
	EventBBGM,
	FaDayResultItem,
	FaDayResults,
	Player,
	PlayerContract,
	TransactionRevert,
} from "../../../common/types.ts";
import { freeAgents, player, team } from "../index.ts";
import { actualPhase } from "../../util/actualPhase.ts";
import { getNumPlayersTradedAwayNormalizedAll } from "./getNumPlayersTradedAwayNormalized.ts";
import { getTeammateJerseyNumbers } from "./genJerseyNumber.ts";
import { recomputeLocalUITeamOvrs } from "../../util/recomputeLocalUITeamOvrs.ts";

type SigningEvent = Extract<EventBBGM, { type: "freeAgent" | "reSigned" }>;

const NO_DATA = "This move doesn't have the data needed to revert it.";

// Whether a season's games are all played, so anything paid or earned in it is
// history rather than something a revert can take back.
const seasonOver = (season: number) =>
	season < g.get("season") ||
	(season === g.get("season") && actualPhase() > PHASE.PLAYOFFS);

// How many rows at the end of his salary log a contract signed with salary
// rows from `start` added, or undefined if the end of the log is no longer
// those rows (edited since, in God Mode).
const contractSalaryRows = (
	p: Player,
	start: number,
	contract: PlayerContract,
) => {
	const numRows = Math.max(0, contract.exp - start + 1);
	if (numRows > p.salaries.length) {
		return;
	}
	const rows = p.salaries.slice(p.salaries.length - numRows);
	for (const [i, row] of rows.entries()) {
		if (row.season !== start + i || row.amount !== contract.amount) {
			return;
		}
	}
	return numRows;
};

// His transactions log without the entry a signing added, if it added one.
const transactionsWithout = (p: Player, eid: number) =>
	(p.transactions ?? []).filter(
		(row) => !(row.type === "freeAgent" && row.eid === eid),
	);

const findReleasedRow = async (
	pid: number,
	tid: number,
	contract: PlayerContract,
) => {
	const rows = await idb.cache.releasedPlayers.getAll();
	return rows.find(
		(row) =>
			row.pid === pid &&
			row.tid === tid &&
			row.contract.amount === contract.amount &&
			row.contract.exp === contract.exp,
	);
};

const signingError = (
	event: SigningEvent,
	revert: TransactionRevert,
	p: Player,
	name: string,
	him: string,
) => {
	const contract = event.contract;
	if (!contract || revert.salaryStart === undefined) {
		return NO_DATA;
	}
	if (p.tid !== event.tids[0]) {
		return `${name} is no longer on the team that signed ${him}.`;
	}
	if (
		p.contract.amount !== contract.amount ||
		p.contract.exp !== contract.exp
	) {
		return `${name} has signed a new contract since.`;
	}
	if (transactionsWithout(p, event.eid).length !== revert.numTransactions) {
		return `${name} has moved since.`;
	}

	// A season of the deal that is over was paid and played under it. Taking
	// the deal back now would have to unwrite that, which a revert doesn't do.
	if (seasonOver(revert.salaryStart)) {
		return `${name} has already played a full season under this contract.`;
	}
	if (contractSalaryRows(p, revert.salaryStart, contract) === undefined) {
		return `${name}'s salary history has been edited since.`;
	}
};

const releaseError = async (
	tid: number,
	revert: TransactionRevert,
	p: Player,
	name: string,
) => {
	if (p.tid !== PLAYER.FREE_AGENT) {
		return `${name} is no longer a free agent.`;
	}
	if ((p.transactions?.length ?? 0) !== revert.numTransactions) {
		return `${name} has moved since.`;
	}

	const t = await idb.cache.teams.get(tid);
	if (!t || t.disabled) {
		return "The team that made this release is no longer active.";
	}

	// Back on the team means back under the old contract, so it has to still be
	// one a rostered player can hold: not expired, and not one the re-signing
	// period has already dealt with.
	const contract = revert.before.contract;
	const season = g.get("season");
	const phase = actualPhase();
	if (
		contract.exp < season ||
		(contract.exp === season && phase >= PHASE.RESIGN_PLAYERS)
	) {
		return `${name}'s old contract has run out.`;
	}

	// The dead money it booked has to still be on the books to take back - or
	// have come off them only because the contract's last season ended.
	if (
		revert.deadMoney &&
		!(await findReleasedRow(p.pid, tid, contract)) &&
		!(contract.exp <= season && phase >= PHASE.DRAFT_LOTTERY)
	) {
		return `${name}'s dead money is no longer on the books.`;
	}
};

const draftPickError = async (
	event: EventBBGM,
	revert: TransactionRevert,
	p: Player,
	name: string,
	him: string,
) => {
	const { dp, contract, salaryStart } = revert;
	if (!dp || !contract || salaryStart === undefined || !revert.before.draft) {
		return NO_DATA;
	}

	// Mid-draft only: once the draft ends, the pick has nowhere to go back to.
	if (g.get("phase") !== PHASE.DRAFT || event.season !== g.get("season")) {
		return "A draft pick can only be reverted during its draft.";
	}
	if (lock.get("drafting")) {
		return "Wait for the picks in progress to finish.";
	}

	if (p.tid !== dp.tid || p.draft.dpid !== dp.dpid) {
		return `${name} is no longer on the team that drafted ${him}.`;
	}
	if (
		p.contract.amount !== contract.amount ||
		p.contract.exp !== contract.exp
	) {
		return `${name} has signed a new contract since.`;
	}
	const transactions = p.transactions ?? [];
	const last = transactions.at(-1);
	if (
		transactions.length !== revert.numTransactions + 1 ||
		last?.type !== "draft" ||
		last.season !== event.season
	) {
		return `${name} has moved since.`;
	}
	if (contractSalaryRows(p, salaryStart, contract) === undefined) {
		return `${name}'s salary history has been edited since.`;
	}

	// A spent pick is deleted, so it existing again means something else
	// already put it back.
	if (await idb.cache.draftPicks.get(dp.dpid)) {
		return "That pick is already back on the board.";
	}
};

// Whether the move behind an event can still be taken back, and the player it
// moved if so. Like a trade, it's all-or-nothing: only while the player is
// exactly where the move left him, under the deal it gave him, with nothing
// done to him since. The moment anything has built on the move, undoing it
// would no longer restore the world before it, just scramble the world after
// it - so it isn't revertable and the reason says what changed.
export const planTransactionRevert = async (
	event: EventBBGM,
): Promise<
	| {
			p: Player;
			revert: TransactionRevert;
	  }
	| {
			error: string;
	  }
> => {
	if (
		event.type !== "freeAgent" &&
		event.type !== "reSigned" &&
		event.type !== "release" &&
		event.type !== "draft"
	) {
		return { error: "Only signings, releases and draft picks revert here." };
	}

	if (g.get("college")) {
		return { error: "Moves can't be reverted in a college league." };
	}

	// Fantasy and expansion drafts rebuild the league around themselves.
	if (g.get("phase") < PHASE.PRESEASON) {
		return { error: "Moves can't be reverted during this draft." };
	}

	const revert = event.revert;
	const pid = event.pids?.[0];
	const tid = event.tids?.[0];
	if (!revert || pid === undefined || tid === undefined) {
		return { error: NO_DATA };
	}

	if (lock.get("gameSim")) {
		return { error: "Wait for the games in progress to finish." };
	}

	// Retired and deleted players are not in the cache, and neither can come
	// back anyway.
	const p = await idb.cache.players.get(pid);
	if (!p) {
		return { error: "That player is no longer in the league." };
	}

	const name = `${p.firstName} ${p.lastName}`;
	const him = helpers.pronoun(g.get("gender"), "him");

	let error;
	if (event.type === "release") {
		error = await releaseError(tid, revert, p, name);
	} else if (event.type === "draft") {
		error = await draftPickError(event, revert, p, name, him);
	} else if (event.type === "freeAgent" || event.type === "reSigned") {
		error = signingError(event, revert, p, name, him);
	}
	if (error !== undefined) {
		return { error };
	}

	return { p, revert };
};

// A team that keeps its roster sorted re-sorts around whoever came or went,
// as after a trade.
const sortRoster = async (tid: number) => {
	const t = await idb.cache.teams.get(tid);
	const onlyNewPlayers =
		g.get("userTids").includes(tid) &&
		!g.get("spectator") &&
		t !== undefined &&
		!t.keepRosterSorted;
	await team.rosterAutoSort(tid, onlyNewPlayers);
};

const restoreJerseyNumber = (p: Player, jerseyNumber: string | undefined) => {
	if (jerseyNumber === undefined) {
		delete p.jerseyNumber;
	} else {
		p.jerseyNumber = jerseyNumber;
	}
};

// In a synced league, a signing off the free agency board also has a line in
// that day's board results saying who won him, on what roll. Undone, the
// signing never happened, so that line goes too.
const forgetBoardResult = async (event: SigningEvent) => {
	const contract = event.contract!;
	const pid = event.pids[0];
	const tid = event.tids[0];
	const isThisSigning = (item: FaDayResultItem) => {
		if (item.type === "contest") {
			return (
				item.pid === pid &&
				item.winnerTid === tid &&
				item.amount === contract.amount &&
				item.exp === contract.exp
			);
		}
		if (item.type === "unopposed") {
			return (
				item.pid === pid &&
				item.tid === tid &&
				item.amount === contract.amount &&
				item.exp === contract.exp
			);
		}
		return false;
	};

	// Results are read on demand rather than cached, so older days are only on
	// disk. The cache's copy of a day wins, being newer.
	const rows = new Map<string, FaDayResults>();
	try {
		const onDisk: FaDayResults[] =
			(await (idb.league as any)?.getAll("faDayResults")) ?? [];
		for (const row of onDisk) {
			rows.set(row.key, row);
		}
	} catch {
		// Nothing on disk to read.
	}
	for (const row of await idb.cache.faDayResults.getAll()) {
		rows.set(row.key, row);
	}

	for (const row of rows.values()) {
		if (row.season !== event.season || !row.items.some(isThisSigning)) {
			continue;
		}
		await idb.cache.faDayResults.put({
			...row,
			items: row.items.filter((item) => !isThisSigning(item)),
		});
	}
};

const revertSigning = async (
	event: SigningEvent,
	revert: TransactionRevert,
	p: Player,
) => {
	const { before } = revert;
	const tid = event.tids[0];

	// Signed straight from free agency, he goes back to exactly how he was
	// there. An AI team re-signing its own player is the one signing of a man
	// who wasn't a free agent; undone, he's the free agent he'd have been if
	// the team had let him walk.
	const wasFreeAgent = before.tid === PLAYER.FREE_AGENT;

	// His asking contract is from the moment he signed. If free agency has
	// moved on since, he asks what the market says now, as a released player
	// does.
	const stale =
		event.season !== g.get("season") || revert.phase !== actualPhase();

	const numRows = contractSalaryRows(p, revert.salaryStart!, event.contract!)!;
	p.salaries = p.salaries.slice(0, p.salaries.length - numRows);

	p.tid = PLAYER.FREE_AGENT;
	p.contract = helpers.deepCopy(before.contract);
	if (!wasFreeAgent) {
		// Only meaningful during re-signing, where he no longer is.
		delete p.contract.rookieResign;
	}
	p.numDaysFreeAgent = wasFreeAgent ? before.numDaysFreeAgent : 0;
	p.gamesUntilTradable = before.gamesUntilTradable;
	p.ptModifier = wasFreeAgent ? before.ptModifier : 1;
	restoreJerseyNumber(p, before.jerseyNumber);
	p.numPlayersTradedAwayNormalized =
		await getNumPlayersTradedAwayNormalizedAll();
	if (p.transactions) {
		p.transactions = transactionsWithout(p, event.eid);
	}
	await idb.cache.players.put(p);

	// Re-signing talks the signing closed reopen, while the re-signing period
	// they belong to is still on.
	if (
		revert.negotiationTid !== undefined &&
		!stale &&
		actualPhase() === PHASE.RESIGN_PLAYERS
	) {
		await idb.cache.negotiations.put({
			pid: p.pid,
			tid: revert.negotiationTid,
			resigning: true,
		});
	}

	if (revert.phase === PHASE.FREE_AGENCY) {
		await forgetBoardResult(event);
	}

	await sortRoster(tid);

	if (stale || !wasFreeAgent) {
		await freeAgents.normalizeContractDemands({
			type: "dummyExpiringContracts",
			pids: [p.pid],
		});
	}
};

const revertRelease = async (
	tid: number,
	revert: TransactionRevert,
	p: Player,
) => {
	const { before } = revert;

	if (revert.deadMoney) {
		const row = await findReleasedRow(p.pid, tid, before.contract);
		if (row) {
			await idb.cache.releasedPlayers.delete(row.rid);
		}
	}

	p.tid = tid;
	p.contract = helpers.deepCopy(before.contract);
	if (before.salaries) {
		p.salaries = helpers.deepCopy(before.salaries);
	}
	p.numDaysFreeAgent = before.numDaysFreeAgent;
	p.gamesUntilTradable = before.gamesUntilTradable;
	p.ptModifier = before.ptModifier;
	p.yearsFreeAgent = before.yearsFreeAgent;
	delete p.numPlayersTradedAwayNormalized;
	restoreJerseyNumber(p, before.jerseyNumber);

	// His number may have gone to a teammate while he was away.
	if (g.get("phase") <= PHASE.PLAYOFFS) {
		const teamJerseyNumbers = await getTeammateJerseyNumbers(tid, [p.pid]);
		if (
			p.jerseyNumber === undefined ||
			teamJerseyNumbers.includes(p.jerseyNumber)
		) {
			player.setJerseyNumber(
				p,
				await player.genJerseyNumber(p, teamJerseyNumbers),
			);
		}
	}

	await idb.cache.players.put(p);

	if (await idb.cache.negotiations.get(p.pid)) {
		await idb.cache.negotiations.delete(p.pid);
	}

	await sortRoster(tid);
};

const revertDraftPick = async (revert: TransactionRevert, p: Player) => {
	const { before } = revert;
	const dp = revert.dp!;

	const numRows = contractSalaryRows(p, revert.salaryStart!, revert.contract!)!;
	p.salaries = p.salaries.slice(0, p.salaries.length - numRows);

	p.tid = before.tid;
	p.draft = helpers.deepCopy(before.draft!);
	p.contract = helpers.deepCopy(before.contract);
	p.numDaysFreeAgent = before.numDaysFreeAgent;
	p.gamesUntilTradable = before.gamesUntilTradable;
	p.ptModifier = before.ptModifier;
	p.yearsFreeAgent = before.yearsFreeAgent;
	restoreJerseyNumber(p, before.jerseyNumber);
	p.transactions = (p.transactions ?? []).slice(0, revert.numTransactions);
	await idb.cache.players.put(p);

	// The pick comes back exactly as it was spent, so its team is on the clock
	// with it again.
	await idb.cache.draftPicks.put(helpers.deepCopy(dp));

	await sortRoster(dp.tid);
};

// Undo the signing, release or draft pick behind an event, God Mode only,
// leaving no trace it ever happened. Returns an error message, or undefined on
// success.
//
// Every move records how the player stood before it on its event (see
// TransactionRevert), and undoing it puts that back rather than running some
// opposite move on top: a revert is not a release, or a signing, or a pick of
// its own, with the money, log lines and roster churn those would bring.
//
//   - A signing: he's a free agent again, asking what he asked, with the
//     contract's salary rows, its transaction entry and any re-signing talks
//     it closed all back as they were.
//   - A release: he's back on the team under his old contract, and the dead
//     money it booked comes off that team's books.
//   - A draft pick: he's back in the draft pool and the pick is back on the
//     board with its team on the clock - so only during that draft.
//
// The event itself is deleted, and in a synced league the delete has to carry
// the event's content, since event ids differ between devices (see
// revertTrade, which this mirrors).
//
// What a revert leaves alone is what already happened: games he played,
// salary already paid for them, other moves made around him in the meantime.
const revertTransaction = async (eid: number): Promise<string | undefined> => {
	if (!g.get("godMode")) {
		return "God Mode is required to revert a move.";
	}

	const event = await idb.getCopy.events({ eid }, "noCopyCache");
	if (!event) {
		return "Move not found.";
	}

	const plan = await planTransactionRevert(event);
	if ("error" in plan) {
		return plan.error;
	}
	const { p, revert } = plan;

	if (event.type === "release") {
		await revertRelease(event.tids![0]!, revert, p);
	} else if (event.type === "draft") {
		await revertDraftPick(revert, p);
	} else if (event.type === "freeAgent" || event.type === "reSigned") {
		await revertSigning(event, revert, p);
	}

	await idb.cache.events.delete(eid);
	// The event may live only on disk (the cache holds recent seasons), in
	// which case the delete above recorded no snapshot and would stay local.
	// Re-record it with the row we already loaded, so the content-matched
	// delete reaches every device whatever the cache held.
	changeTracker.record("events", eid, "delete", event);

	await toUI("realtimeUpdate", [["playerMovement"]]);
	await recomputeLocalUITeamOvrs();
	if (event.type === "draft") {
		await updatePlayMenu();
	}
};

export default revertTransaction;
