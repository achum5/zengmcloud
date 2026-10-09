import { g } from "../util/index.ts";
import { resolveFeedAccounts } from "../util/socialFeed.ts";
import { idb } from "../db/index.ts";
import { BUILT_IN_ARCHETYPES } from "../../common/socialPersonality.ts";
import { defineView, type ViewArgs } from "../util/defineView.ts";
import type { RouteParams } from "../../ui/router/types.ts";

const processInputs = (params: RouteParams<"socialAccounts">) => ({
	// Optional: the manage page doubles as the editor, opening straight onto
	// one account when a link points at it.
	handle: params.handle,
});

const updateSocialAccounts = async ({
	inputs,
	updateEvents,
	prevInputs,
}: ViewArgs<typeof processInputs>) => {
	if (
		updateEvents.has("firstRun") ||
		updateEvents.has("gameSim") ||
		prevInputs?.handle !== inputs.handle
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

export default defineView({
	id: "socialAccounts",
	processInputs,
	load: updateSocialAccounts,
});
