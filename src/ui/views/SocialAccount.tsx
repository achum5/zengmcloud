import { useMemo, useState } from "react";
import clsx from "clsx";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import type { View } from "../../common/types.ts";
import {
	Avatar,
	Icon,
	SocialPost,
	VerifiedBadge,
	type TeamLike,
} from "./SocialPost.tsx";

// ONE ACCOUNT'S PAGE. Cover photo, profile picture, name, handle, bio, where
// they are and who they follow, and everything they have said lately - posts
// on one tab, the replies they left under other people's on the other.
//
// The cover falls back to the team's colours rather than to a grey box,
// because a fan account with no picture should still look like it belongs to
// its team - and nobody is going to upload five hundred cover photos.

const ROLE_LABEL: Record<string, string> = {
	insider: "Insider",
	beatWriter: "Beat writer",
	nationalPundit: "Columnist",
	analytics: "Analytics",
	homerFan: "Fan",
	doomerFan: "Fan",
	casualFan: "Fan",
	troll: "Fan",
	aggregator: "Wire",
	teamOfficial: "Official",
	player: "Player",
	capNerd: "Cap analyst",
	draftHead: "Draft analyst",
	historian: "Historian",
	localRadio: "Radio",
};

const SocialAccount = ({
	account,
	errorMessage,
	pictures,
	posts,
	replies,
	season,
	suggested,
	team,
	teams,
}: View<"socialAccount">) => {
	useTitleBar({
		title: account ? `${account.name} (@${account.handle})` : "Account",
	});

	const [tab, setTab] = useState<"posts" | "replies">("posts");
	const [following, setFollowing] = useState(false);

	const teamByTid = useMemo(
		() => new Map<number, TeamLike>((teams ?? []).map((t: any) => [t.tid, t])),
		[teams],
	);

	if (errorMessage || !account) {
		return <p className="alert alert-danger d-inline-block">{errorMessage}</p>;
	}

	const colors = team?.colors ?? ["#555555", "#888888", "#cccccc"];
	const cover = account.coverUrl
		? { backgroundImage: `url(${account.coverUrl})` }
		: {
				background: `linear-gradient(135deg, ${colors[0]} 0%, ${colors[1]} 100%)`,
			};

	const teamLike: TeamLike | undefined = team as TeamLike | undefined;
	const role = ROLE_LABEL[account.archetypeId] ?? "Media";

	return (
		<div className="social-layout" data-social-ready>
			<div className="social-main">
				<div className="social-header">
					<div className="social-header-row">
						<a
							className="social-back"
							href={helpers.leagueUrl(["social"])}
							title="Back"
						>
							<Icon name="back" />
						</a>
						<div className="flex-grow-1" style={{ minWidth: 0 }}>
							<div className="social-header-title">{account.name}</div>
							<div className="social-header-sub">
								{account.postCount} {account.postCount === 1 ? "post" : "posts"}
							</div>
						</div>
					</div>
				</div>

				<div className="social-cover" style={cover} />

				<div className="social-profile-top">
					<div className="social-avatar-xl">
						<Avatar
							account={account as any}
							picture={pictures?.[account.id]}
							size={126}
							team={teamLike}
						/>
					</div>
					<div className="social-profile-actions">
						<a
							className="btn-social-outline"
							href={helpers.leagueUrl(["social_accounts", account.handle])}
						>
							Edit
						</a>
						<button
							className={clsx("btn-social-follow", following && "following")}
							onClick={() => setFollowing((f) => !f)}
							type="button"
						>
							{following ? "Following" : "Follow"}
						</button>
					</div>
				</div>

				<div className="social-profile-body">
					<div className="social-profile-name">
						{account.name}
						{account.verified ? <VerifiedBadge size={20} /> : null}
					</div>
					<div className="social-profile-handle">@{account.handle}</div>
					<span className="social-role">{role}</span>

					{account.bio ? <div className="social-bio">{account.bio}</div> : null}

					<div className="social-profile-meta">
						{team ? (
							<span>
								<Icon name="pin" /> {team.region}
							</span>
						) : null}
						{team ? (
							<span>
								<Icon name="link" />
								<a
									href={helpers.leagueUrl([
										"roster",
										`${team.abbrev}_${team.tid}`,
									])}
								>
									{team.region} {team.name}
								</a>
							</span>
						) : null}
						{account.pid !== undefined ? (
							<span>
								<Icon name="link" />
								<a href={helpers.leagueUrl(["player", account.pid])}>
									Player page
								</a>
							</span>
						) : null}
						<span>
							<Icon name="calendar" /> Joined {account.joined}
						</span>
					</div>

					<div className="social-profile-counts">
						<span>
							<b>{account.following}</b> Following
						</span>
						<span>
							<b>{account.followers}</b> Followers
						</span>
					</div>
				</div>

				<div className="social-tabs">
					<button
						className={clsx("social-tab", tab === "posts" && "active")}
						onClick={() => setTab("posts")}
						type="button"
					>
						Posts
					</button>
					<button
						className={clsx("social-tab", tab === "replies" && "active")}
						onClick={() => setTab("replies")}
						type="button"
					>
						Replies
					</button>
				</div>

				{tab === "posts" ? (
					posts.length === 0 ? (
						<div className="social-empty">
							<div className="social-empty-title">Quiet lately</div>
							Nothing from this account in the last month of {season}.
						</div>
					) : (
						posts.map((post: any) => (
							<SocialPost
								key={post.id}
								account={{ ...post, verified: account.verified }}
								engagement={post.engagement}
								meta={post.day === 0 ? "Offseason" : `Day ${post.day}`}
								picture={pictures?.[account.id]}
								team={teamLike}
								text={post.text}
								time={post.time}
							/>
						))
					)
				) : replies.length === 0 ? (
					<div className="social-empty">
						<div className="social-empty-title">No replies yet</div>
						Nothing under anyone else&rsquo;s posts in the last week.
					</div>
				) : (
					replies.map((reply: any) => (
						<div key={reply.id}>
							<SocialPost
								account={reply.parent}
								compact
								engagement={reply.parent.engagement}
								meta={reply.day === 0 ? "Offseason" : `Day ${reply.day}`}
								picture={pictures?.[reply.parent.accountId]}
								team={
									reply.parent.tid === undefined
										? undefined
										: teamByTid.get(reply.parent.tid)
								}
								text={reply.parent.text}
								threadBelow
								time={reply.parent.time}
							/>
							<SocialPost
								account={{ ...reply, verified: account.verified }}
								engagement={reply.engagement}
								picture={pictures?.[account.id]}
								quote={reply.quote}
								replyTo={reply.replyTo ?? reply.parent.handle}
								team={teamLike}
								text={reply.text}
								threadAbove
								time={reply.time}
							/>
						</div>
					))
				)}
			</div>

			<aside className="social-side">
				{suggested && suggested.length > 0 ? (
					<div className="social-card">
						<h3>You might like</h3>
						{suggested
							.filter((a: any) => a.accountId !== account.id)
							.map((a: any) => (
								<div key={a.accountId} className="social-suggest">
									<a href={helpers.leagueUrl(["social", a.handle])}>
										<Avatar
											account={a}
											picture={pictures?.[a.accountId]}
											size={40}
											team={
												a.tid === undefined ? undefined : teamByTid.get(a.tid)
											}
										/>
									</a>
									<div className="social-suggest-body">
										<a
											className="social-name"
											href={helpers.leagueUrl(["social", a.handle])}
										>
											{a.name} {a.verified ? <VerifiedBadge /> : null}
										</a>
										<span className="social-handle">@{a.handle}</span>
									</div>
									<a
										className="btn-social-follow"
										href={helpers.leagueUrl(["social", a.handle])}
									>
										Follow
									</a>
								</div>
							))}
					</div>
				) : null}
				<div className="social-foot">
					<a href={helpers.leagueUrl(["social"])}>Back to the feed</a> ·{" "}
					<a href={helpers.leagueUrl(["social_accounts", account.handle])}>
						Edit this account
					</a>
				</div>
			</aside>
		</div>
	);
};

export default SocialAccount;
