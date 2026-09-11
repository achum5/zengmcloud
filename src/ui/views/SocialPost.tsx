import clsx from "clsx";
import { Fragment, type ReactNode } from "react";
import { PlayerPicture } from "../components/PlayerPicture.tsx";
import { helpers } from "../util/helpers.ts";
import {
	formatCount,
	type AccountPicture,
	type Engagement,
} from "../../common/socialMetrics.ts";

// ONE POST, and everything visual about an account lives here so the feed,
// an account page and every embed cannot drift apart.
//
// The avatar is derived rather than uploaded, because seven hundred accounts
// cannot each be given a picture by hand: a player shows the face the league
// already generated for him, a franchise shows its logo, and everyone else
// gets a monogram tinted with their team's colour. Any of the three is
// overridden by a custom image URL when one is set in the editor.

export type PostAccount = {
	accountId: string;
	handle: string;
	name: string;
	kind: "player" | "team" | "media";
	archetypeId?: string;
	tid?: number;
	pid?: number;
	avatarUrl?: string;
	verified?: boolean;
};

export type TeamLike = {
	tid: number;
	abbrev: string;
	imgURL?: string;
	colors?: [string, string, string];
};

// A stable colour per handle, for the accounts with no team to borrow one
// from. Hue only: saturation and lightness are fixed so every monogram sits at
// the same weight next to real logos.
const monogramHue = (handle: string) => {
	let h = 0;
	for (let i = 0; i < handle.length; i++) {
		h = (h * 31 + handle.charCodeAt(i)) % 360;
	}
	return h;
};

const initials = (name: string) =>
	name
		.split(/\s+/)
		.slice(0, 2)
		.map((part) => part[0] ?? "")
		.join("")
		.toUpperCase();

// facesjs draws head and shoulders at 2:3, with the eyes about a third of the
// way down. An avatar wants the head, so the face is rendered oversized inside
// a circular window showing roughly the top tenth to the top seven-tenths of
// the drawing - the same crop a profile picture gets when somebody uploads a
// photo taken from the waist up. The numbers were read off a rendered avatar
// rather than guessed; the first attempt showed a forehead.
const FaceAvatar = ({
	picture,
	size,
}: {
	picture: AccountPicture;
	size: number;
}) => (
	<div
		className="rounded-circle overflow-hidden flex-shrink-0 position-relative bg-body-secondary"
		style={{ width: size, height: size }}
	>
		<div
			className="position-absolute"
			style={{
				width: size * 1.11,
				height: size * 1.667,
				left: size * -0.055,
				top: size * -0.167,
			}}
		>
			<PlayerPicture
				colors={picture.colors}
				face={picture.face as any}
				imgURL={picture.imgURL}
				jersey={picture.jersey}
				lazy
			/>
		</div>
	</div>
);

export const Avatar = ({
	account,
	team,
	size = 40,
	picture,
}: {
	account: PostAccount;
	team?: TeamLike;
	size?: number;
	picture?: AccountPicture;
}) => {
	const style = { width: size, height: size };

	if (account.avatarUrl) {
		return (
			<img
				alt=""
				className="rounded-circle flex-shrink-0"
				src={account.avatarUrl}
				style={{ ...style, objectFit: "cover" }}
			/>
		);
	}

	if (picture?.imgURL !== undefined || picture?.face !== undefined) {
		return <FaceAvatar picture={picture} size={size} />;
	}

	const logoURL = picture?.logoURL ?? team?.imgURL;
	if (account.kind === "team" && logoURL !== undefined) {
		return (
			<div
				className="rounded-circle d-flex align-items-center justify-content-center flex-shrink-0 bg-body-secondary p-1"
				style={style}
			>
				<img
					alt=""
					src={logoURL}
					style={{ maxWidth: "100%", maxHeight: "100%" }}
				/>
			</div>
		);
	}

	// A media or fan account borrows its team's colour, so the local beat
	// writer reads as local at a glance.
	const colors = picture?.colors ?? team?.colors;
	const background =
		colors?.[0] ?? `hsl(${monogramHue(account.handle)}, 45%, 42%)`;
	return (
		<div
			className="rounded-circle d-flex align-items-center justify-content-center flex-shrink-0 fw-bold"
			style={{
				...style,
				backgroundColor: background,
				fontSize: size / 2.6,
				// Not Bootstrap's text-white: the dark theme redefines $white
				// as near-black, which made every monogram vanish at night.
				color: "#fff",
			}}
		>
			{initials(account.name)}
		</div>
	);
};

export const VerifiedBadge = ({ size = 16 }: { size?: number }) => (
	<svg
		aria-label="Verified"
		className="flex-shrink-0"
		height={size}
		role="img"
		style={{ verticalAlign: "-0.15em" }}
		viewBox="0 0 24 24"
		width={size}
	>
		<path
			d="M22.25 12c0-1.43-.88-2.67-2.19-3.34.46-1.39.2-2.9-.81-3.91s-2.52-1.27-3.91-.81C14.67 2.63 13.43 1.75 12 1.75s-2.67.88-3.34 2.19c-1.39-.46-2.9-.2-3.91.81s-1.27 2.52-.81 3.91C2.63 9.33 1.75 10.57 1.75 12s.88 2.67 2.19 3.34c-.46 1.39-.2 2.9.81 3.91s2.52 1.27 3.91.81c.67 1.31 1.91 2.19 3.34 2.19s2.67-.88 3.34-2.19c1.39.46 2.9.2 3.91-.81s1.27-2.52.81-3.91c1.31-.67 2.19-1.91 2.19-3.34z"
			fill="#1d9bf0"
		/>
		<path
			d="M10.87 15.75 7.5 12.38l1.34-1.34 2.03 2.03 4.29-4.29 1.34 1.34z"
			fill="#fff"
		/>
	</svg>
);

// ---------------------------------------------------------------- ICONS
//
// Drawn inline so they carry currentColor and need no icon font.

export const Icon = ({ name }: { name: string }) => {
	switch (name) {
		case "reply":
			return (
				<svg viewBox="0 0 24 24" aria-hidden="true">
					<path d="M1.751 10c0-4.42 3.584-8 8.005-8h4.366c4.49 0 8.129 3.64 8.129 8.13 0 2.96-1.607 5.68-4.196 7.11l-8.054 4.46v-3.69h-.067c-4.49.1-8.183-3.51-8.183-8.01zm8.005-6c-3.317 0-6.005 2.69-6.005 6 0 3.37 2.77 6.08 6.138 6.01l.351-.01h1.761v2.3l5.087-2.81c1.951-1.08 3.163-3.13 3.163-5.36 0-3.39-2.744-6.13-6.129-6.13H9.756z" />
				</svg>
			);
		case "repost":
			return (
				<svg viewBox="0 0 24 24" aria-hidden="true">
					<path d="M4.5 3.88l4.432 4.14-1.364 1.46L5.5 7.55V16c0 1.1.896 2 2 2H13v2H7.5c-2.209 0-4-1.79-4-4V7.55L1.432 9.48.068 8.02 4.5 3.88zM16.5 6H11V4h5.5c2.209 0 4 1.79 4 4v8.45l2.068-1.93 1.364 1.46-4.432 4.14-4.432-4.14 1.364-1.46 2.068 1.93V8c0-1.1-.896-2-2-2z" />
				</svg>
			);
		case "like":
			return (
				<svg viewBox="0 0 24 24" aria-hidden="true">
					<path d="M16.697 5.5c-1.222-.06-2.679.51-3.89 2.16l-.805 1.09-.806-1.09C9.984 6.01 8.526 5.44 7.304 5.5c-1.243.07-2.349.78-2.91 1.91-.552 1.12-.633 2.78.479 4.82 1.074 1.97 3.257 4.27 7.129 6.61 3.87-2.34 6.052-4.64 7.126-6.61 1.111-2.04 1.03-3.7.477-4.82-.561-1.13-1.666-1.84-2.908-1.91zm4.187 7.69c-1.351 2.48-4.001 5.12-8.379 7.67l-.503.3-.504-.3c-4.379-2.55-7.029-5.19-8.382-7.67-1.36-2.5-1.41-4.86-.514-6.67.887-1.79 2.647-2.91 4.601-3.01 1.651-.09 3.368.56 4.798 2.01 1.429-1.45 3.146-2.1 4.796-2.01 1.954.1 3.714 1.22 4.601 3.01.896 1.81.846 4.17-.514 6.67z" />
				</svg>
			);
		case "views":
			return (
				<svg viewBox="0 0 24 24" aria-hidden="true">
					<path d="M8.75 21V3h2v18h-2zM18 21V8.5h2V21h-2zM4 21l.004-10h2L6 21H4zm9.248 0v-7h2v7h-2z" />
				</svg>
			);
		case "bookmark":
			return (
				<svg viewBox="0 0 24 24" aria-hidden="true">
					<path d="M4 4.5C4 3.12 5.119 2 6.5 2h11C18.881 2 20 3.12 20 4.5v18.44l-8-5.71-8 5.71V4.5zM6.5 4c-.276 0-.5.22-.5.5v14.56l6-4.29 6 4.29V4.5c0-.28-.224-.5-.5-.5h-11z" />
				</svg>
			);
		case "share":
			return (
				<svg viewBox="0 0 24 24" aria-hidden="true">
					<path d="M12 2.59l5.7 5.7-1.41 1.42L13 6.41V16h-2V6.41l-3.3 3.3-1.41-1.42L12 2.59zM21 15l-.02 3.51c0 1.38-1.12 2.49-2.5 2.49H5.5C4.11 21 3 19.88 3 18.5V15h2v3.5c0 .28.22.5.5.5h12.98c.28 0 .5-.22.5-.5L19 15h2z" />
				</svg>
			);
		case "search":
			return (
				<svg viewBox="0 0 24 24" aria-hidden="true">
					<path d="M10.25 3.75c-3.59 0-6.5 2.91-6.5 6.5s2.91 6.5 6.5 6.5c1.795 0 3.419-.726 4.596-1.904 1.178-1.177 1.904-2.801 1.904-4.596 0-3.59-2.91-6.5-6.5-6.5zm-8.5 6.5c0-4.694 3.806-8.5 8.5-8.5s8.5 3.806 8.5 8.5c0 1.986-.682 3.815-1.824 5.262l4.781 4.781-1.414 1.414-4.781-4.781c-1.447 1.142-3.276 1.824-5.262 1.824-4.694 0-8.5-3.806-8.5-8.5z" />
				</svg>
			);
		case "back":
			return (
				<svg viewBox="0 0 24 24" aria-hidden="true">
					<path d="M7.414 13l5.043 5.04-1.414 1.42L3.586 12l7.457-7.46 1.414 1.42L7.414 11H21v2H7.414z" />
				</svg>
			);
		case "pin":
			return (
				<svg viewBox="0 0 24 24" aria-hidden="true">
					<path d="M12 7c-1.93 0-3.5 1.57-3.5 3.5S10.07 14 12 14s3.5-1.57 3.5-3.5S13.93 7 12 7zm0 5c-.827 0-1.5-.673-1.5-1.5S11.173 9 12 9s1.5.673 1.5 1.5S12.827 12 12 12zm0-10c-4.687 0-8.5 3.813-8.5 8.5 0 5.967 7.621 11.174 7.945 11.393l.555.375.555-.375C12.879 21.674 20.5 16.467 20.5 10.5 20.5 5.813 16.687 2 12 2zm0 17.849C10.312 18.586 5.5 14.499 5.5 10.5 5.5 6.916 8.416 4 12 4s6.5 2.916 6.5 6.5c0 3.999-4.812 8.086-6.5 9.349z" />
				</svg>
			);
		case "link":
			return (
				<svg viewBox="0 0 24 24" aria-hidden="true">
					<path d="M18.36 5.64c-1.95-1.96-5.11-1.96-7.07 0L9.88 7.05 8.46 5.64l1.42-1.42c2.73-2.73 7.16-2.73 9.9 0 2.73 2.74 2.73 7.17 0 9.9l-1.42 1.42-1.41-1.42 1.41-1.41c1.96-1.96 1.96-5.12 0-7.07zm-2.12 3.53l-7.07 7.07-1.41-1.41 7.07-7.07 1.41 1.41zm-12.02.71l1.42-1.42 1.41 1.42-1.41 1.41c-1.96 1.96-1.96 5.12 0 7.07 1.95 1.96 5.11 1.96 7.07 0l1.41-1.41 1.42 1.41-1.42 1.42c-2.73 2.73-7.16 2.73-9.9 0-2.73-2.74-2.73-7.17 0-9.9z" />
				</svg>
			);
		case "calendar":
			return (
				<svg viewBox="0 0 24 24" aria-hidden="true">
					<path d="M7 4V3h2v1h6V3h2v1h1.5C19.89 4 21 5.12 21 6.5v12c0 1.38-1.11 2.5-2.5 2.5h-13C4.12 21 3 19.88 3 18.5v-12C3 5.12 4.12 4 5.5 4H7zm0 2H5.5c-.27 0-.5.22-.5.5v12c0 .28.23.5.5.5h13c.28 0 .5-.22.5-.5v-12c0-.28-.22-.5-.5-.5H17v1h-2V6H9v1H7V6zm0 6h2v-2H7v2zm0 4h2v-2H7v2zm4-4h2v-2h-2v2zm0 4h2v-2h-2v2zm4-4h2v-2h-2v2z" />
				</svg>
			);
		case "quote":
			return (
				<svg viewBox="0 0 24 24" aria-hidden="true">
					<path d="M14.23 2.854c.98-.977 2.56-.977 3.54 0l3.38 3.378c.97.977.97 2.559 0 3.536L9.91 21H3v-6.91L14.23 2.854zm2.12 1.414c-.19-.195-.51-.195-.7 0L5 14.918V19h4.09L19.73 8.354c.2-.196.2-.512 0-.708l-3.38-3.378z" />
				</svg>
			);
		default:
			return null;
	}
};

// ---------------------------------------------------------------- TEXT
//
// Hashtags and handles are the two things a post links, and the blue is what
// makes a line of text read as a post rather than a sentence.
const TOKEN = /(#\w+|@\w+)/g;

export const PostText = ({ text }: { text: string }) => {
	const parts = text.split(TOKEN);
	return (
		<div className="social-text">
			{parts.map((part, i) => {
				if (i % 2 === 0) {
					return <Fragment key={i}>{part}</Fragment>;
				}
				if (part.startsWith("@")) {
					return (
						<a
							key={i}
							className="social-link"
							href={helpers.leagueUrl(["social", part.slice(1)])}
						>
							{part}
						</a>
					);
				}
				return (
					<span key={i} className="social-link">
						{part}
					</span>
				);
			})}
		</div>
	);
};

const Count = ({ value }: { value: number }) => (
	<span className="social-count">{value > 0 ? formatCount(value) : ""}</span>
);

const Actions = ({ engagement }: { engagement?: Engagement }) => {
	const e = engagement ?? { likes: 0, reposts: 0, replies: 0, views: 0 };
	return (
		<div className="social-actions">
			<span className="social-action" title={`${e.replies} replies`}>
				<Icon name="reply" />
				<Count value={e.replies} />
			</span>
			<span
				className="social-action social-action-repost"
				title={`${e.reposts} reposts`}
			>
				<Icon name="repost" />
				<Count value={e.reposts} />
			</span>
			<span
				className="social-action social-action-like"
				title={`${e.likes} likes`}
			>
				<Icon name="like" />
				<Count value={e.likes} />
			</span>
			<span className="social-action" title={`${e.views} views`}>
				<Icon name="views" />
				<Count value={e.views} />
			</span>
			<span className="social-actions-end">
				<span className="social-action" title="Bookmark">
					<Icon name="bookmark" />
				</span>
				<span className="social-action" title="Share">
					<Icon name="share" />
				</span>
			</span>
		</div>
	);
};

export const SocialPost = ({
	account,
	text,
	team,
	picture,
	meta,
	time,
	engagement,
	quote,
	compact,
	replyTo,
	// Part of a thread: a line drops from this avatar to the next post
	// (below), or this post continues one above and shares its hairline.
	threadBelow,
	threadAbove,
	children,
}: {
	account: PostAccount;
	text: string;
	team?: TeamLike;
	picture?: AccountPicture;
	meta?: string;
	time?: string;
	engagement?: Engagement;
	quote?: boolean;
	compact?: boolean;
	replyTo?: string;
	threadBelow?: boolean;
	threadAbove?: boolean;
	children?: ReactNode;
}) => {
	const profile = helpers.leagueUrl(["social", account.handle]);
	return (
		<article
			className={clsx(
				"social-post",
				compact && "social-compact",
				threadBelow && "social-post-threaded",
				threadAbove && "social-post-continued",
			)}
			data-post-kind={account.kind}
			data-post-arch={account.archetypeId}
		>
			<div className="social-avatar-col">
				<a href={profile} className="d-block">
					<Avatar
						account={account}
						team={team}
						picture={picture}
						size={compact ? 36 : 40}
					/>
				</a>
				{threadBelow ? <div className="social-thread-line" /> : null}
			</div>
			<div className="social-post-body">
				<div className="social-post-head">
					<a className="social-name" href={profile}>
						{account.name}
					</a>
					{account.verified ? <VerifiedBadge /> : null}
					<a className="social-handle" href={profile}>
						@{account.handle}
					</a>
					{time ? (
						<>
							<span className="social-dot">·</span>
							<span className="social-time">{time}</span>
						</>
					) : null}
					{meta ? (
						<>
							<span className="social-dot">·</span>
							<span className="social-meta">{meta}</span>
						</>
					) : null}
				</div>
				{replyTo ? (
					<div className="social-replying">
						Replying to{" "}
						<a href={helpers.leagueUrl(["social", replyTo])}>@{replyTo}</a>
					</div>
				) : null}
				{quote ? (
					<div className="social-quote-label">
						<Icon name="quote" /> Quoted
					</div>
				) : null}
				<PostText text={text} />
				{children}
				<Actions engagement={engagement} />
			</div>
		</article>
	);
};

// A post and the replies hanging off it, drawn as one thread.
export const SocialThread = ({
	post,
	pictures,
	teamByTid,
	compact,
}: {
	post: {
		id: string;
		accountId: string;
		handle: string;
		name: string;
		kind: "player" | "team" | "media";
		archetypeId?: string;
		tid?: number;
		pid?: number;
		text: string;
		time?: string;
		verified?: boolean;
		engagement?: Engagement;
		replies?: {
			id: string;
			accountId: string;
			handle: string;
			name: string;
			kind: "player" | "team" | "media";
			archetypeId?: string;
			tid?: number;
			pid?: number;
			text: string;
			time?: string;
			verified?: boolean;
			quote?: boolean;
			replyTo?: string;
			engagement?: Engagement;
		}[];
	};
	pictures: Record<string, AccountPicture | undefined>;
	teamByTid: Map<number, TeamLike>;
	compact?: boolean;
}) => {
	const replies = post.replies ?? [];
	return (
		<>
			<SocialPost
				account={post}
				compact={compact}
				engagement={post.engagement}
				picture={pictures[post.accountId]}
				team={post.tid === undefined ? undefined : teamByTid.get(post.tid)}
				text={post.text}
				threadBelow={replies.length > 0}
				time={post.time}
			/>
			{replies.map((reply, i) => (
				<SocialPost
					key={reply.id}
					account={reply}
					compact={compact}
					engagement={reply.engagement}
					picture={pictures[reply.accountId]}
					quote={reply.quote}
					replyTo={reply.replyTo ?? post.handle}
					team={reply.tid === undefined ? undefined : teamByTid.get(reply.tid)}
					text={reply.text}
					threadAbove
					threadBelow={i < replies.length - 1}
					time={reply.time}
				/>
			))}
		</>
	);
};
