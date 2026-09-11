import { useMemo } from "react";
import { helpers } from "../util/helpers.ts";
import { SocialThread, type TeamLike } from "../views/SocialPost.tsx";
import type { AccountPicture } from "../../common/socialMetrics.ts";

// THE FEED ON ANOTHER PAGE. The same posts the timeline shows, in a box that
// fits wherever it is put - the dashboard, a player page, a box score - with
// a link through to the full thing. Rendered only when the page was handed
// posts, so a league with the feed off never sees an empty frame.

export const SocialEmbed = ({
	title,
	posts,
	pictures,
	teams,
	moreHref,
	moreText = "See more",
	emptyText,
	showDay = true,
}: {
	title: string;
	posts: any[];
	pictures: Record<string, AccountPicture | undefined>;
	teams: TeamLike[];
	moreHref?: string;
	moreText?: string;
	emptyText?: string;
	showDay?: boolean;
}) => {
	const teamByTid = useMemo(
		() => new Map<number, TeamLike>(teams.map((t) => [t.tid, t])),
		[teams],
	);

	if (posts.length === 0 && !emptyText) {
		return null;
	}

	return (
		<div className="social-embed" data-social-embed>
			<div className="social-embed-head">
				<span>{title}</span>
				<a href={moreHref ?? helpers.leagueUrl(["social"])}>{moreText}</a>
			</div>
			{posts.length === 0 ? (
				<div className="social-empty">{emptyText}</div>
			) : (
				posts.map((post) => (
					<SocialThread
						key={post.id}
						compact
						pictures={pictures}
						post={{
							...post,
							time:
								showDay && post.day !== undefined
									? `${post.time} · ${post.day === 0 ? "Offseason" : `Day ${post.day}`}`
									: post.time,
						}}
						teamByTid={teamByTid}
					/>
				))
			)}
		</div>
	);
};
