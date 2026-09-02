import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import type { View } from "../../common/types.ts";
import {
	Avatar,
	SocialPost,
	VerifiedBadge,
	type TeamLike,
} from "./SocialPost.tsx";

// ONE ACCOUNT'S PAGE. Cover photo, profile picture, name, handle, bio, who
// they follow, and everything they have said lately.
//
// The cover falls back to the team's colours rather than to a grey box,
// because a fan account with no picture should still look like it belongs to
// its team - and nobody is going to upload five hundred cover photos.
const SocialAccount = ({
	account,
	errorMessage,
	pictures,
	posts,
	season,
	team,
}: View<"socialAccount">) => {
	useTitleBar({ title: account ? account.name : "Account" });

	if (errorMessage || !account) {
		return <p className="alert alert-danger d-inline-block">{errorMessage}</p>;
	}

	const colors = team?.colors ?? ["#555555", "#888888", "#cccccc"];
	const cover = account.coverUrl
		? { backgroundImage: `url(${account.coverUrl})`, backgroundSize: "cover" }
		: {
				background: `linear-gradient(135deg, ${colors[0]} 0%, ${colors[1]} 100%)`,
			};

	const teamLike: TeamLike | undefined = team as TeamLike | undefined;

	return (
		<>
			<div className="rounded-top" style={{ ...cover, height: 140 }} />

			<div className="px-3" style={{ marginTop: -36 }}>
				<div className="d-flex align-items-end gap-3">
					<div className="rounded-circle border border-3 border-body bg-body">
						<Avatar
							account={account as any}
							picture={pictures?.[account.id]}
							size={84}
							team={teamLike}
						/>
					</div>
					<div className="pb-2">
						<h2 className="mb-0 d-flex align-items-center gap-2">
							{account.name}
							{account.verified ? <VerifiedBadge size={20} /> : null}
						</h2>
						<div className="text-body-secondary">
							@{account.handle}
							{account.followers ? <> · {account.followers} followers</> : null}
						</div>
					</div>
				</div>

				{account.bio ? <p className="mt-3 mb-1">{account.bio}</p> : null}

				<p className="text-body-secondary small">
					{account.kind === "player" ? "Player" : null}
					{account.kind === "team" ? "Team" : null}
					{account.kind === "media" ? "Media" : null}
					{team ? (
						<>
							{" · "}
							<a
								href={helpers.leagueUrl([
									"roster",
									`${team.abbrev}_${team.tid}`,
								])}
							>
								{team.region} {team.name}
							</a>
						</>
					) : null}
					{account.pid !== undefined ? (
						<>
							{" · "}
							<a href={helpers.leagueUrl(["player", account.pid])}>Profile</a>
						</>
					) : null}
					{" · "}
					<a href={helpers.leagueUrl(["social_accounts", account.handle])}>
						Edit
					</a>
				</p>
			</div>

			<hr />

			{posts.length === 0 ? (
				<p className="text-body-secondary">
					Nothing from this account in the last month of {season}.
				</p>
			) : null}

			<div className="d-flex flex-column gap-2">
				{posts.map((post: any) => (
					<div key={post.id} className="border rounded p-3">
						<SocialPost
							account={{ ...post, verified: account.verified }}
							engagement={post.engagement}
							meta={post.day === 0 ? "Offseason" : `Day ${post.day}`}
							picture={pictures?.[account.id]}
							team={teamLike}
							text={post.text}
							time={post.time}
						/>
					</div>
				))}
			</div>
		</>
	);
};

export default SocialAccount;
