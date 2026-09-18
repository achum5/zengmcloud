import { useMemo } from "react";
import { MoreLinks } from "../components/MoreLinks.tsx";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import type { View } from "../../common/types.ts";
import { SafeHtml } from "../components/SafeHtml.tsx";
import { SocialThread } from "./SocialPost.tsx";

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
			teamsAndAll: abbrev,
			seasonsAndAll: season,
			eventType,
		},
	});

	const teamByTid = useMemo(
		() => (social ? new Map(social.teams.map((t) => [t.tid, t])) : undefined),
		[social],
	);

	const moreLinks =
		abbrev !== "all" ? (
			<MoreLinks
				type="team"
				page="depth"
				abbrev={abbrev}
				tid={tid}
				season={season !== "all" ? season : undefined}
			/>
		) : (
			<p>
				More: <a href={helpers.leagueUrl(["news", "all", season])}>News Feed</a>
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
							<SafeHtml dirty={e.text} />
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
