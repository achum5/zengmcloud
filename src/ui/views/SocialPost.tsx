import clsx from "clsx";
import { PlayerPicture } from "../components/PlayerPicture.tsx";
import { helpers } from "../util/helpers.ts";
import {
	formatCount,
	type AccountPicture,
} from "../../common/socialMetrics.ts";

// ONE POST, and everything visual about an account lives here so the feed and
// an account page cannot drift apart.
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
	size = 44,
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
			className="rounded-circle d-flex align-items-center justify-content-center flex-shrink-0 text-white fw-bold"
			style={{
				...style,
				backgroundColor: background,
				fontSize: size / 2.6,
			}}
		>
			{initials(account.name)}
		</div>
	);
};

export const VerifiedBadge = ({ size = 14 }: { size?: number }) => (
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

const Stat = ({
	one,
	many,
	value,
}: {
	one: string;
	many: string;
	value: number;
}) =>
	// A post nobody touched shows nothing rather than three zeros, which is
	// what every real client does and what stops a quiet post looking broken.
	value > 0 ? (
		<span className="text-body-secondary small" title={`${value} ${many}`}>
			{formatCount(value)} {value === 1 ? one : many}
		</span>
	) : null;

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
	indent,
	replyTo,
}: {
	account: PostAccount;
	text: string;
	team?: TeamLike;
	picture?: AccountPicture;
	meta?: string;
	time?: string;
	engagement?: { likes: number; reposts: number; replies: number };
	quote?: boolean;
	compact?: boolean;
	// An answer to another answer, stepped in so the argument reads as one.
	indent?: boolean;
	replyTo?: string;
}) => (
	<div
		className={clsx("d-flex", compact ? "gap-2" : "gap-3")}
		style={indent ? { marginLeft: 20 } : undefined}
	>
		<Avatar
			account={account}
			team={team}
			picture={picture}
			size={compact ? 32 : 44}
		/>
		<div className="flex-grow-1" style={{ minWidth: 0 }}>
			<div className="d-flex flex-wrap align-items-baseline gap-1">
				<a
					className="fw-bold text-body text-decoration-none"
					href={helpers.leagueUrl(["social", account.handle])}
				>
					{account.name}
				</a>
				{account.verified ? <VerifiedBadge /> : null}
				<a
					className="text-body-secondary small text-decoration-none"
					href={helpers.leagueUrl(["social", account.handle])}
				>
					@{account.handle}
				</a>
				{time ? (
					<span className="text-body-secondary small">· {time}</span>
				) : null}
				{meta ? (
					<span className="text-body-secondary small">· {meta}</span>
				) : null}
				{quote ? (
					<span className="badge text-bg-light border ms-1">quoted</span>
				) : null}
			</div>
			{replyTo ? (
				<div className="text-body-secondary small">
					Replying to{" "}
					<a
						className="text-body-secondary"
						href={helpers.leagueUrl(["social", replyTo])}
					>
						@{replyTo}
					</a>
				</div>
			) : null}
			<div style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>
				{text}
			</div>
			{engagement &&
			engagement.likes + engagement.reposts + engagement.replies > 0 ? (
				<div className="d-flex gap-3 mt-1">
					<Stat many="replies" one="reply" value={engagement.replies} />
					<Stat many="reposts" one="repost" value={engagement.reposts} />
					<Stat many="likes" one="like" value={engagement.likes} />
				</div>
			) : null}
		</div>
	</div>
);
