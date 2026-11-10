import { gameAttributesArrayToObject } from "../../../../common/gameAttributesArrayToObject.ts";
import { unwrapGameAttribute } from "../../../../common/unwrapGameAttribute.ts";
import type { Phase } from "../../../../common/types.ts";
import type { VersionChangeTransaction } from "../../connectLeague.ts";
import {
	addLegacyEvent,
	addLegacyReleasedPlayer,
	createLegacyContractInfoByPid,
	getFractionCurrentSeasonPaid,
	isLegacyPlayerRecord,
	removeStrayLegacyFields,
	upgradeLegacyPlayer,
} from "./upgradeLegacyPlayers.ts";

// Upstream's version 75 (p.contracts), renumbered to run after this fork's own
// 75-80. See convertLegacyContracts.
//
// In a synced league, this device may already have received some players in
// the new shape (from a league mate on a newer build) before it upgraded.
// Those are left alone, and so are the releasedPlayers rows for them: their
// releases are already recorded on their contracts.
export const migrate81 = async (transaction: VersionChangeTransaction) => {
	const gameAttributes = gameAttributesArrayToObject(
		await transaction.objectStore("gameAttributes").getAll(),
	);
	const season: number = gameAttributes.season;
	const phase: Phase = gameAttributes.phase;
	const numGames = unwrapGameAttribute(gameAttributes, "numGames");

	const teamSeasons = await transaction
		.objectStore("teamSeasons")
		.index("season, tid")
		.getAll(IDBKeyRange.bound([season], [season, ""]));
	const leagueState = {
		fractionCurrentSeasonPaid: getFractionCurrentSeasonPaid(
			teamSeasons,
			numGames,
		),
		phase,
		season,
	};

	const info = createLegacyContractInfoByPid();

	const releasedPlayers: unknown[] = await (
		transaction.objectStore("releasedPlayers" as any) as any
	).getAll();
	for (const rp of releasedPlayers) {
		addLegacyReleasedPlayer(info, rp);
	}

	for await (const cursor of transaction.objectStore("events")) {
		addLegacyEvent(info, cursor.value);
	}

	for await (const cursor of transaction.objectStore("players")) {
		const p = cursor.value as any;
		if (!isLegacyPlayerRecord(p)) {
			if (!Array.isArray(p.contracts)) {
				// Not even an empty salary log, so there is nothing to convert
				p.contracts = [];
				await cursor.update(p);
			} else if (removeStrayLegacyFields(p)) {
				await cursor.update(p);
			}
			continue;
		}

		upgradeLegacyPlayer(p, leagueState, info);

		await cursor.update(p);
	}
};
