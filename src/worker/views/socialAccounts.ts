import { g } from "../util/index.ts";
import { resolveFeedAccounts } from "../util/socialFeed.ts";
import { idb } from "../db/index.ts";
import { BUILT_IN_ARCHETYPES } from "../../common/socialPersonality.ts";
import type { UpdateEvents, ViewInput } from "../../common/types.ts";

const updateSocialAccounts = async (
	inputs: ViewInput<"socialAccounts">,
	updateEvents: UpdateEvents,
	state: any,
) => {
	if (
		updateEvents.includes("firstRun") ||
		updateEvents.includes("gameSim") ||
		state.handle !== inputs.handle
	) {
		if (!g.get("socialFeed")) {
			return {
				errorMessage:
					"The League Feed is turned off for this league. Turn it on in League Settings under UI.",
			};
		}

		const accounts = (await resolveFeedAccounts()).map((account) => ({
			id: account.id,
			handle: account.handle,
			name: account.name,
			bio: account.bio,
			kind: account.kind,
			tid: account.tid,
			pid: account.pid,
			archetypeId: account.archetypeId,
			avatarUrl: account.avatarUrl,
			coverUrl: account.coverUrl,
			implicit: account.implicit,
			postiness: account.personality.postiness,
			tone: account.personality.tone,
		}));

		const teams = (await idb.cache.teams.getAll()).map((t) => ({
			tid: t.tid,
			abbrev: t.abbrev,
			region: t.region,
			name: t.name,
			imgURL: t.imgURL,
			colors: t.colors,
			disabled: t.disabled,
		}));

		return {
			accounts,
			archetypes: BUILT_IN_ARCHETYPES.map((a) => ({
				id: a.id,
				label: a.label,
				summary: a.summary,
			})),
			// Which account the editor opened on, if any.
			handle: inputs.handle,
			teams,
		};
	}
};

export default updateSocialAccounts;
