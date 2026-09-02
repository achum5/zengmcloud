import clsx from "clsx";
import { PlayerPicture } from "../components/PlayerPicture.tsx";
import { helpers } from "../util/helpers.ts";

// ONE POST, and everything visual about an account lives here so the feed and
// an account page cannot drift apart.
//
// The avatar is derived rather than uploaded, because five hundred accounts
// cannot each be given a picture by hand: a player shows the face the league
// already generated for him, a team shows its logo, and everyone else gets a
// monogram on a colour derived from the handle. Any of the three is overridden
// by a custom image URL when one is set.

export type PostAccount = {
	accountId: string;
	handle: string;
	name: string;
	kind: "player" | "team" | "media";
	tid?: number;
	pid?: number;
	avatarUrl?: string;
};

export type TeamLike = {
	tid: number;
	abbrev: string;
	imgURL?: string;
	colors?: [string, string, string];
};

// A stable colour per handle, so an account looks the same everywhere without
// storing anything. Hue only: saturation and lightness are fixed so every
// monogram sits at the same weight next to real logos.
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

export const Avatar = ({
	account,
	team,
	size = 44,
	face,
}: {
	account: PostAccount;
	team?: TeamLike;
	size?: number;
	face?: any;
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

	if (account.kind === "player" && face) {
		return (
			<div
				className="rounded-circle overflow-hidden flex-shrink-0 bg-body-secondary"
				style={style}
			>
				<PlayerPicture face={face} imgURL={undefined} />
			</div>
		);
	}

	if (account.kind === "team" && team?.imgURL) {
		return (
			<div
				className="rounded-circle d-flex align-items-center justify-content-center flex-shrink-0 bg-body-secondary p-1"
				style={style}
			>
				<img
					alt=""
					src={team.imgURL}
					style={{ maxWidth: "100%", maxHeight: "100%" }}
				/>
			</div>
		);
	}

	return (
		<div
			className="rounded-circle d-flex align-items-center justify-content-center flex-shrink-0 text-white fw-bold"
			style={{
				...style,
				backgroundColor: `hsl(${monogramHue(account.handle)}, 45%, 42%)`,
				fontSize: size / 2.6,
			}}
		>
			{initials(account.name)}
		</div>
	);
};

export const SocialPost = ({
	account,
	text,
	team,
	face,
	meta,
	quote,
	compact,
}: {
	account: PostAccount;
	text: string;
	team?: TeamLike;
	face?: any;
	meta?: string;
	quote?: boolean;
	compact?: boolean;
}) => (
	<div className={clsx("d-flex", compact ? "gap-2" : "gap-3")}>
		<Avatar
			account={account}
			team={team}
			face={face}
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
				<a
					className="text-body-secondary small text-decoration-none"
					href={helpers.leagueUrl(["social", account.handle])}
				>
					@{account.handle}
				</a>
				{meta ? (
					<span className="text-body-secondary small">· {meta}</span>
				) : null}
				{quote ? (
					<span className="badge text-bg-light border ms-1">quoted</span>
				) : null}
			</div>
			<div style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>
				{text}
			</div>
		</div>
	</div>
);
