import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import type { View } from "../../common/types.ts";
import { SocialPost, type TeamLike } from "./SocialPost.tsx";

// THE TIMELINE.
//
// Grouped by day rather than shown as one endless column, because a league's
// day IS its unit of time: everything in a day happened at once, and pretending
// otherwise by inventing timestamps would be a lie the rest of the app would
// have to keep.
const SocialFeed = ({
	accountCount,
	days,
	errorMessage,
	feed,
	hasMore,
	season,
	teams,
}: View<"socialFeed">) => {
	useTitleBar({ title: "League Feed" });

	if (errorMessage) {
		return <p className="alert alert-warning d-inline-block">{errorMessage}</p>;
	}

	const teamByTid = new Map<number, TeamLike>(
		teams.map((t: any) => [t.tid, t]),
	);

	const totalPosts = feed.reduce(
		(sum: number, day: any) => sum + day.posts.length,
		0,
	);

	return (
		<>
			<p className="text-body-secondary">
				{accountCount} accounts ·{" "}
				<a href={helpers.leagueUrl(["social_accounts"])}>Manage</a>
			</p>

			{totalPosts === 0 ? <p>Nothing has happened yet this season.</p> : null}

			{feed.map((day: any) => (
				<div key={day.day} className="mb-4">
					<div className="d-flex align-items-center gap-2 mb-2">
						<h3 className="mb-0">
							{day.day === 0 ? "Offseason" : `Day ${day.day}`}
						</h3>
						<div className="flex-grow-1 border-bottom" />
					</div>

					{day.posts.length === 0 ? (
						<p className="text-body-secondary">Quiet night.</p>
					) : null}

					{day.posts.map((post: any) => (
						<div
							key={post.id}
							className="border rounded p-3 mb-2 bg-body-secondary bg-opacity-25"
						>
							<SocialPost
								account={post}
								team={
									post.tid === undefined ? undefined : teamByTid.get(post.tid)
								}
								text={post.text}
							/>
							{post.replies.length > 0 ? (
								<div className="mt-3 ps-3 border-start d-flex flex-column gap-3">
									{post.replies.map((reply: any) => (
										<SocialPost
											key={reply.id}
											account={reply}
											compact
											quote={reply.quote}
											team={
												reply.tid === undefined
													? undefined
													: teamByTid.get(reply.tid)
											}
											text={reply.text}
										/>
									))}
								</div>
							) : null}
						</div>
					))}
				</div>
			))}

			{hasMore ? (
				<button
					className="btn btn-light-bordered"
					onClick={() => {
						realtimeUpdate(
							[],
							helpers.leagueUrl(["social", String(season), String(days + 4)]),
						);
					}}
					type="button"
				>
					Load earlier days
				</button>
			) : null}
		</>
	);
};

export default SocialFeed;
