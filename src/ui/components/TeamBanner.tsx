import clsx from "clsx";
import {
	useEffect,
	useLayoutEffect,
	useState,
	type ComponentProps,
	type CSSProperties,
	type ReactNode,
	type Ref,
} from "react";
import { contrast } from "../../common/stockCourts.ts";
import { TeamLogoInline } from "./TeamLogoInline.tsx";
import { TitleMenus } from "./Controller/TitleBar.tsx";
import { TeamNameDropdown } from "./Dropdown.tsx";
import { local, localActions, useLocal } from "../util/local.ts";
import { toWorker } from "../util/toWorker.ts";

// A TEAM'S COLORS AS A BANNER: the team's first color the ground, the words
// on it white (or near black, on a light one), and a second color - one that
// stands out from the ground - for the stripes.
const HEX = /^#[\da-f]{6}$/i;
export const bannerColors = (colors: string[] | undefined) => {
	const list = (colors ?? []).filter((c) => HEX.test(c));
	const bg = list[0] ?? "#2c3e50";
	const fg = contrast("#ffffff", bg) >= 2.6 ? "#ffffff" : "#16181b";
	const accent =
		list.slice(1).find((c) => contrast(c, bg) >= 1.8) ??
		(fg === "#ffffff" ? "#ffffff" : "#16181b");
	return { bg, fg, accent };
};

const vars = (colors: string[] | undefined, prefix = "") => {
	const { bg, fg, accent } = bannerColors(colors);
	return {
		[`--tb${prefix}-bg`]: bg,
		[`--tb${prefix}-fg`]: fg,
		[`--tb${prefix}-accent`]: accent,
	} as CSSProperties;
};

const Stripes = ({ className }: { className: string }) => (
	<div className={clsx("tb-stripes", className)} aria-hidden>
		<span />
		<span />
		<span />
	</div>
);

type BannerTeam = {
	tid: number;
	abbrev: string;
	region: string;
	name: string;
	imgURL?: string;
	imgURLSmall?: string;
	colors?: string[];
};

const link = (href: string | undefined, children: ReactNode) =>
	href ? <a href={href}>{children}</a> : children;

// ONE TEAM'S BANNER: its logo big, its city over its name, a line under it,
// and anything else to the right (a player's details, say).
export const TeamBanner = ({
	t,
	href,
	sub,
	picture,
	kicker,
	title,
	heading,
	menus,
	noLogo,
	children,
	className,
	bannerRef,
}: {
	bannerRef?: Ref<HTMLDivElement>;
	t: BannerTeam;
	noLogo?: boolean;
	// Over it all on a phone (a page's title, the banner in place of the
	// title bar), and the page's dropdowns (top right; on a phone by that).
	heading?: ReactNode;
	menus?: ReactNode;
	href?: string;
	// Under the name.
	sub?: ReactNode;
	// Between the logo and the words: a player's picture.
	picture?: ReactNode;
	// Over the name, and the name - the team's own unless given.
	kicker?: ReactNode;
	title?: ReactNode;
	children?: ReactNode;
	className?: string;
}) => (
	<div className={clsx("tb", className)} style={vars(t.colors)} ref={bannerRef}>
		<Stripes className="tb-stripes-top" />
		<div className="tb-inner">
			{heading ? <div className="tb-phone-head">{heading}</div> : null}
			{menus ? <div className="tb-menus">{menus}</div> : null}
			<div className={noLogo ? "d-none" : "tb-logo"}>
				{link(
					href,
					<TeamLogoInline
						alt={t.abbrev}
						imgURL={t.imgURL}
						imgURLSmall={t.imgURL ?? t.imgURLSmall}
						includePlaceholderIfNoLogo
						size={120}
					/>,
				)}
			</div>
			{picture ? <div className="tb-picture">{picture}</div> : null}
			<div className="tb-words">
				<div className="tb-kicker">{kicker ?? t.region}</div>
				<div className="tb-title">{title ?? link(href, t.name)}</div>
				{sub ? <div className="tb-sub">{sub}</div> : null}
			</div>
			{children ? <div className="tb-side">{children}</div> : null}
		</div>
		<Stripes className="tb-stripes-bottom" />
	</div>
);

// TWO TEAMS, ONE BANNER: each half in its team's colors, split on a slant;
// each side its logo, city and name and a line under them, and in the
// middle the two numbers (the score, the wins in a series) with a tag over
// them (the round) and a word under each.
export const MatchupBanner = ({
	teams,
	numbers,
	under,
	tag,
	status,
	hrefs,
	subs,
	won,
	extra,
	leaders,
	className,
}: {
	// Each side's top man (the box score: its top scorer).
	leaders?: [ReactNode, ReactNode];
	// Under the tag and the status, in the middle (the score by quarter).
	extra?: ReactNode;
	// Left, right.
	teams: [BannerTeam, BannerTeam];
	numbers?: [ReactNode, ReactNode];
	under?: [ReactNode, ReactNode];
	tag?: ReactNode;
	status?: ReactNode;
	hrefs?: [string | undefined, string | undefined];
	subs?: [ReactNode, ReactNode];
	// Which side won - the other's number dimmed.
	won?: 0 | 1;
	className?: string;
}) => (
	<div
		className={clsx("mb-banner", className)}
		style={{ ...vars(teams[0].colors, "0"), ...vars(teams[1].colors, "1") }}
	>
		{([0, 1] as const).map((i) => {
			const t = teams[i];
			return (
				// (Its class written out whole: the production build keeps only
				// the class names it finds in the code as they are.)
				<div
					key={i}
					className={clsx("mb-half", i === 0 ? "mb-half-0" : "mb-half-1")}
				>
					<Stripes className="tb-stripes-top" />
					<div className="mb-team">
						<div className="mb-logo">
							{link(
								hrefs?.[i],
								<TeamLogoInline
									alt={t.abbrev}
									imgURL={t.imgURL}
									imgURLSmall={t.imgURL ?? t.imgURLSmall}
									includePlaceholderIfNoLogo
									size={96}
								/>,
							)}
						</div>
						<div className="mb-words">
							<div className="tb-kicker">{t.region}</div>
							<div className="mb-name">
								{link(
									hrefs?.[i],
									<>
										<span className="mb-name-full">{t.name}</span>
										<span className="mb-name-short">{t.abbrev}</span>
									</>,
								)}
							</div>
							{subs?.[i] ? <div className="tb-sub">{subs[i]}</div> : null}
							{leaders?.[i] ? (
								<div className="mb-leader mb-leader-side">{leaders[i]}</div>
							) : null}
						</div>
					</div>
					{numbers ? (
						<div
							className={clsx("mb-number", {
								"mb-number-lost": won !== undefined && won !== i,
							})}
						>
							<div className="mb-big">{numbers[i]}</div>
							{under?.[i] ? <div className="mb-under">{under[i]}</div> : null}
						</div>
					) : null}
					<Stripes className="tb-stripes-bottom" />
				</div>
			);
		})}
		{leaders ? (
			<div className="mb-leaders">
				<div className="mb-leader">{leaders[0]}</div>
				<div className="mb-leader">{leaders[1]}</div>
			</div>
		) : null}
		{tag || status || extra ? (
			<div className="mb-middle">
				{tag ? <div className="mb-tag">{tag}</div> : null}
				{status ? <div className="mb-status">{status}</div> : null}
				{extra ? <div className="mb-extra">{extra}</div> : null}
			</div>
		) : null}
	</div>
);

// A PAGE'S BAR, PINNED: once the element `watch` names scrolls up under the
// top bar, a slim bar in the team's colors held there (the player page: his
// name, his team). No room taken until then.
export const PinnedBar = ({
	watch,
	colors,
	children,
}: {
	watch: string;
	colors: string[] | undefined;
	children: ReactNode;
}) => {
	const [shown, setShown] = useState(false);
	useEffect(() => {
		const el = document.querySelector(watch);
		if (!el || typeof IntersectionObserver === "undefined") {
			return;
		}
		const observer = new IntersectionObserver(
			([e]) => {
				setShown(!!e && !e.isIntersecting && e.boundingClientRect.top < 0);
			},
			{ rootMargin: "-60px 0px 0px 0px" },
		);
		observer.observe(el);
		return () => {
			observer.disconnect();
		};
	}, [watch]);
	return (
		<div className="recap-scorebar-pin">
			<div
				className={shown ? "pin-bar pin-bar-on" : "pin-bar"}
				style={vars(colors)}
				aria-hidden={!shown}
			>
				{children}
			</div>
		</div>
	);
};

// Two teams' colors (as MatchupBanner sets them), for anything else in them.
export const matchupVars = (
	colors0: string[] | undefined,
	colors1: string[] | undefined,
) => ({ ...vars(colors0, "0"), ...vars(colors1, "1") }) as CSSProperties;

// A PAGE'S OWN BANNER: in place of the title bar (hidden while it is up),
// the page's dropdowns in it, and its title over it on a phone.
export const useHideTitleBar = (active = true) => {
	useLayoutEffect(() => {
		if (!active) {
			return;
		}
		localActions.update({
			titleBarHidden: local.getState().titleBarHidden + 1,
		});
		return () => {
			localActions.update({
				titleBarHidden: Math.max(0, local.getState().titleBarHidden - 1),
			});
		};
	}, [active]);
};

export const PageBanner = ({
	teamName,
	...props
}: Omit<ComponentProps<typeof TeamBanner>, "menus"> & {
	// The team's name the page's team dropdown (a team's pages).
	teamName?: boolean;
}) => {
	useHideTitleBar();
	const { dropdown } = useLocal(["dropdown"]);
	return (
		<TeamBanner
			{...props}
			title={
				teamName ? (
					<TeamNameDropdown dropdown={dropdown}>
						{props.title ?? props.t.name}
					</TeamNameDropdown>
				) : (
					props.title
				)
			}
			menus={<TitleMenus skipTeams={teamName} />}
		/>
	);
};

// A team's colors, asked of the worker once a league and kept.
const colorsCache = new Map<string, string[]>();
export const useTeamColors = (tid: number | undefined) => {
	const { lid } = useLocal(["lid"]);
	const key = `${lid}:${tid}`;
	const [colors, setColors] = useState(() => colorsCache.get(key));
	useEffect(() => {
		if (tid === undefined || tid < 0) {
			setColors(undefined);
			return;
		}
		const have = colorsCache.get(key);
		if (have) {
			setColors(have);
			return;
		}
		// (Asked again if the worker doesn't answer - busy syncing, say - rather
		// than left in the stand-in gray.)
		let live = true;
		(async () => {
			for (let attempt = 0; attempt < 5 && live; attempt += 1) {
				try {
					const got = await toWorker("main", "getTeamColorsUI", tid);
					if (got) {
						colorsCache.set(key, got);
						if (live) {
							setColors(got);
						}
						return;
					}
				} catch {
					// Again, in a moment.
				}
				await new Promise((resolve) => {
					setTimeout(resolve, 700 * (attempt + 1));
				});
			}
		})();
		return () => {
			live = false;
		};
	}, [key, tid]);
	return colors;
};

// Every team's colors at once (standings, the playoffs: a team's own spot in
// its colors), asked of the worker once a league and kept.
// (Asked once, however many spots ask at once; not at all in the classic
// look - on false.)
const allColorsCache = new Map<number, Record<number, string[]>>();
const allColorsAsked = new Map<number, Promise<Record<number, string[]>>>();
export const useAllTeamColors = (on = true) => {
	const { lid } = useLocal(["lid"]);
	const [colors, setColors] = useState(() =>
		lid === undefined ? undefined : allColorsCache.get(lid),
	);
	useEffect(() => {
		if (lid === undefined || !on) {
			return;
		}
		const have = allColorsCache.get(lid);
		if (have) {
			setColors(have);
			return;
		}
		let live = true;
		(async () => {
			let asked = allColorsAsked.get(lid);
			if (!asked) {
				asked = toWorker("main", "getAllTeamColorsUI", undefined);
				allColorsAsked.set(lid, asked);
			}
			const got = await asked;
			allColorsCache.set(lid, got);
			if (live) {
				setColors(got);
			}
		})();
		return () => {
			live = false;
		};
	}, [lid, on]);
	return colors;
};

// A team's own spot in its colors: the CSS variables for it.
export const brandVars = (colors: string[] | undefined) => vars(colors);
