import clsx from "clsx";
import {
	useEffect,
	useState,
	type CSSProperties,
	type MouseEvent,
	type ReactNode,
} from "react";
import { helpers } from "../util/helpers.ts";
import {
	Avatar,
	VerifiedBadge,
	type PostAccount,
	type TeamLike,
} from "./SocialPost.tsx";
import type { AccountPicture } from "../../common/socialMetrics.ts";

// THE FRAME AROUND THE FEED, laid out the way the old Twitter web app was: a
// column of big icon links on the left with you at the bottom of it, the
// timeline in the middle, and a search box on the right. Shared by the
// timeline and every profile so moving between them never shifts the page.
//
// Every link here goes somewhere real. There is no compose box and no Tweet
// button, because nobody in a league writes the posts - the feed does.
//
// The columns fold the way the real one did as the page narrows, by the width
// of the page area rather than the window, since the game's own sidebar may
// or may not be open beside it: labels drop to icons, then the right column
// goes, and on a phone the nav becomes a bar across the top of the timeline.

export type SocialSection =
	| "home"
	| "explore"
	| "notifications"
	| "profile"
	| "accounts";

export type SocialMe = PostAccount;

const NavIcon = ({
	name,
	active,
}: {
	name: SocialSection;
	active: boolean;
}) => {
	const common = {
		viewBox: "0 0 24 24",
		"aria-hidden": true,
		fill: active ? "currentColor" : "none",
		stroke: "currentColor",
		strokeWidth: 1.9,
		strokeLinejoin: "round" as const,
		strokeLinecap: "round" as const,
	};
	switch (name) {
		case "home":
			return (
				<svg {...common}>
					<path d="M12 2.9 3 10.1v9.9c0 .8.7 1.5 1.5 1.5h5V15h5v6.5h5c.8 0 1.5-.7 1.5-1.5v-9.9L12 2.9z" />
				</svg>
			);
		case "explore":
			return (
				<svg {...common} fill="none" strokeWidth={active ? 2.9 : 1.9}>
					<path d="M4.5 9h16M3.5 15h16M10.5 3.5l-2 17M15.5 3.5l-2 17" />
				</svg>
			);
		case "notifications":
			return (
				<svg {...common}>
					<path d="M12 2.8a6.2 6.2 0 0 0-6.2 6.2v3.4l-1.7 3.9c-.2.5.1 1 .7 1h14.4c.5 0 .9-.5.7-1l-1.7-3.9V9A6.2 6.2 0 0 0 12 2.8z" />
					<path d="M9.4 20.2a2.7 2.7 0 0 0 5.2 0" fill="none" />
				</svg>
			);
		case "profile":
			return (
				<svg {...common}>
					<circle cx="12" cy="7.8" r="4.3" />
					<path d="M3.8 21.2c0-4.4 3.6-7 8.2-7s8.2 2.6 8.2 7z" />
				</svg>
			);
		case "accounts":
			return (
				<svg {...common}>
					<circle cx="9" cy="8" r="3.7" />
					<path d="M2.3 20.5c0-3.9 3-6.1 6.7-6.1s6.7 2.2 6.7 6.1z" />
					<path
						d="M15.6 4.6a3.4 3.4 0 0 1 0 6.6M18 14.6c2.3.6 3.7 2.6 3.7 5.4"
						fill="none"
					/>
				</svg>
			);
	}
};

// The mark at the top of the nav. A basketball rather than anybody's bird:
// the same solid blue, cut through with seams in the page colour.
export const SocialMark = () => (
	<svg className="social-mark" viewBox="0 0 24 24" aria-hidden="true">
		<circle cx="12" cy="12" r="10.5" fill="currentColor" />
		<path
			d="M12 1.5v21M1.5 12h21M5.1 4.1c2.4 2.2 3.6 5 3.6 7.9s-1.2 5.7-3.6 7.9M18.9 4.1c-2.4 2.2-3.6 5-3.6 7.9s1.2 5.7 3.6 7.9"
			fill="none"
			stroke="var(--social-bg)"
			strokeWidth="1.3"
		/>
	</svg>
);

export const sectionHref = (section: SocialSection, me?: SocialMe) => {
	switch (section) {
		case "home":
			return helpers.leagueUrl(["social"]);
		case "explore":
			return `${helpers.leagueUrl(["social"])}#explore`;
		case "notifications":
			return `${helpers.leagueUrl(["social"])}#notifications`;
		case "profile":
			return me
				? helpers.leagueUrl(["social", me.handle])
				: helpers.leagueUrl(["social"]);
		case "accounts":
			return helpers.leagueUrl(["social_accounts"]);
	}
};

const LABELS: Record<SocialSection, string> = {
	home: "Home",
	explore: "Explore",
	notifications: "Notifications",
	profile: "Profile",
	accounts: "Accounts",
};

// On the timeline, Home, Explore and Notifications switch the middle column
// in place; everywhere else they are plain links to it.
const sectionClick =
	(section: SocialSection, onSection?: (section: SocialSection) => void) =>
	(event: MouseEvent<HTMLAnchorElement>) => {
		if (
			onSection === undefined ||
			(section !== "home" &&
				section !== "explore" &&
				section !== "notifications") ||
			event.button !== 0 ||
			event.metaKey ||
			event.ctrlKey ||
			event.shiftKey ||
			event.altKey
		) {
			return;
		}
		event.preventDefault();
		onSection(section);
	};

const Badge = ({ count }: { count?: number }) =>
	count !== undefined && count > 0 ? (
		<span className="social-badge">{count > 20 ? "20+" : count}</span>
	) : null;

type MeProps = {
	me?: SocialMe;
	pictures?: Record<string, AccountPicture | undefined>;
	teamByTid?: Map<number, TeamLike>;
};

const MeAvatar = ({
	me,
	pictures,
	teamByTid,
	size,
}: MeProps & { me: SocialMe; size: number }) => (
	<Avatar
		account={me}
		picture={pictures?.[me.accountId]}
		size={size}
		team={me.tid === undefined ? undefined : teamByTid?.get(me.tid)}
	/>
);

export const SocialShell = ({
	active,
	onSection,
	me,
	pictures,
	teamByTid,
	notifications,
	side,
	children,
}: MeProps & {
	active?: SocialSection;
	onSection?: (section: SocialSection) => void;
	notifications?: number;
	side?: ReactNode;
	children: ReactNode;
}) => {
	// The headers and side columns pin just under the game's navbar, however
	// tall it is. And the body is marked for the dark build's black, which
	// runs past the feed to the page edges.
	const [top, setTop] = useState(0);
	useEffect(() => {
		document.body.classList.add("social-page");
		const measure = () => {
			const nav = document.querySelector<HTMLElement>(".navbar.sticky-top");
			setTop(nav?.offsetHeight ?? 0);
		};
		measure();
		window.addEventListener("resize", measure);
		return () => {
			document.body.classList.remove("social-page");
			window.removeEventListener("resize", measure);
		};
	}, []);

	const sections: SocialSection[] = [
		"home",
		"explore",
		"notifications",
		...(me ? (["profile"] as const) : []),
		"accounts",
	];

	return (
		<div
			className="social-layout"
			data-social-ready
			style={{ "--social-top": `${top}px` } as CSSProperties}
		>
			<div className="social-cols">
				<nav className="social-nav" aria-label="League Feed">
					<a
						className="social-nav-logo"
						href={sectionHref("home")}
						onClick={sectionClick("home", onSection)}
						title="Home"
					>
						<SocialMark />
					</a>
					{sections.map((section) => (
						<a
							key={section}
							aria-current={active === section ? "page" : undefined}
							className={clsx(
								"social-nav-item",
								active === section && "active",
							)}
							href={sectionHref(section, me)}
							onClick={sectionClick(section, onSection)}
							title={LABELS[section]}
						>
							<span className="social-nav-icon">
								<NavIcon active={active === section} name={section} />
								{section === "notifications" ? (
									<Badge count={notifications} />
								) : null}
							</span>
							<span className="social-nav-label">{LABELS[section]}</span>
						</a>
					))}
					{me ? (
						<a className="social-me" href={sectionHref("profile", me)}>
							<MeAvatar
								me={me}
								pictures={pictures}
								size={40}
								teamByTid={teamByTid}
							/>
							<span className="social-me-text">
								<span className="social-me-name">
									<span className="text-truncate">{me.name}</span>
									{me.verified ? <VerifiedBadge size={15} /> : null}
								</span>
								<span className="social-me-handle">@{me.handle}</span>
							</span>
						</a>
					) : null}
				</nav>
				<div className="social-main">{children}</div>
				<aside className="social-side">{side}</aside>
			</div>
		</div>
	);
};

// THE NAV ON A PHONE, as the top row of the timeline's sticky header: you on
// the left, the mark in the middle (back to Home), and the two other places
// the timeline can show on the right.
export const SocialTopBar = ({
	active,
	onSection,
	me,
	pictures,
	teamByTid,
	notifications,
}: MeProps & {
	active: SocialSection;
	onSection?: (section: SocialSection) => void;
	notifications?: number;
}) => (
	<div className="social-topbar">
		<div className="social-topbar-start">
			{me ? (
				<a href={sectionHref("profile", me)} title="Profile">
					<MeAvatar
						me={me}
						pictures={pictures}
						size={32}
						teamByTid={teamByTid}
					/>
				</a>
			) : null}
		</div>
		<a
			className="social-topbar-logo"
			href={sectionHref("home")}
			onClick={sectionClick("home", onSection)}
			title="Home"
		>
			<SocialMark />
		</a>
		<div className="social-topbar-end">
			{(["explore", "notifications"] as const).map((section) => (
				<a
					key={section}
					className={clsx("social-topbar-icon", active === section && "active")}
					href={sectionHref(section)}
					onClick={sectionClick(section, onSection)}
					title={LABELS[section]}
				>
					<NavIcon active={active === section} name={section} />
					{section === "notifications" ? <Badge count={notifications} /> : null}
				</a>
			))}
		</div>
	</div>
);
