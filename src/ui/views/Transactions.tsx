import { useMemo, useState } from "react";
import { MoreLinks } from "../components/MoreLinks.tsx";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import type { View } from "../../common/types.ts";
import { SafeHtml } from "../components/SafeHtml.tsx";
import { SocialThread } from "./SocialPost.tsx";
import { toWorker } from "../util/toWorker.ts";
import { confirm } from "../util/confirm.tsx";
import { showNotification } from "../util/showNotification.ts";

const CONFIRM_TEXT: Record<string, string> = {
	trade: "Revert this trade? Everything it moved goes back.",
	freeAgent: "Revert this signing? The player goes back to free agency.",
	reSigned: "Revert this signing? The player goes back to free agency.",
	release: "Revert this release? The player goes back to the team.",
	draft: "Revert this pick? The player goes back into the draft.",
};

// God Mode: take a move back. The worker re-checks the move and refuses with a
// reason if it can no longer be undone.
const RevertButton = ({ eid, type }: { eid: number; type: string }) => {
	const [reverting, setReverting] = useState(false);

	return (
		<button
			className="btn btn-god-mode btn-sm ms-2 flex-shrink-0"
			disabled={reverting}
			onClick={async () => {
				const proceed = await confirm(CONFIRM_TEXT[type] ?? "Revert this?", {
					okText: "Revert",
				});
				if (!proceed) {
					return;
				}
				setReverting(true);
				const error = await toWorker(
					"main",
					type === "trade" ? "revertTrade" : "revertTransaction",
					eid,
				);
				setReverting(false);
				if (error) {
					showNotification({
						type: "error",
						text: error,
					});
				}
			}}
		>
			Revert
		</button>
	);
};

const Transactions = ({
	abbrev,
	eventType,
	events,
	season,
	social,
	tid,
}: View<"transactions">) => {
	useTitleBar({
		title: "Transactions",
		dropdownView: "transactions",
		dropdownFields: {
			teamsAndAllWatch: abbrev,
			seasonsAndAll: season,
			eventType,
		},
	});

	const teamByTid = useMemo(
		() => (social ? new Map(social.teams.map((t) => [t.tid, t])) : undefined),
		[social],
	);

	const moreLinks =
		tid !== undefined ? (
			<MoreLinks
				type="team"
				page="transactions"
				abbrev={abbrev}
				tid={tid}
				season={season !== "all" ? season : undefined}
			/>
		) : (
			<p>
				More:{" "}
				<a href={helpers.leagueUrl(["news", abbrev, season])}>News Feed</a>
			</p>
		);

	return (
		<>
			{moreLinks}

			<ul className="list-group">
				{events.map((e) => {
					// What the feed said about this move, hung under the log line.
					const posts = social?.postsByEid[e.eid];
					return (
						<li key={e.eid} className="list-group-item">
							{e.revertable ? (
								<div className="d-flex align-items-start">
									<div className="flex-grow-1">
										<SafeHtml dirty={e.text} />
									</div>
									<RevertButton eid={e.eid} type={e.type} />
								</div>
							) : (
								<SafeHtml dirty={e.text} />
							)}
							{posts && posts.length > 0 && teamByTid ? (
								<div className="border-top mt-2 pt-2">
									{posts.map((post) => (
										<SocialThread
											key={post.id}
											compact
											pictures={social!.pictures}
											post={post}
											teamByTid={teamByTid}
										/>
									))}
								</div>
							) : null}
						</li>
					);
				})}
			</ul>
		</>
	);
};

export default Transactions;
