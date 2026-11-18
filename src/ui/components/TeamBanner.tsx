import clsx from "clsx";
import {
	useEffect,
	useLayoutEffect,
	useState,
	type CSSProperties,
	type ReactNode,
} from "react";
import { contrast } from "../../common/stockCourts.ts";
import { TeamLogoInline } from "./TeamLogoInline.tsx";
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
	navs,
	leaders,
	className,
}: {
	// Each side's arrows to its games before and after (the game's banner),
	// in place of the stripes at its foot.
	navs?: [ReactNode, ReactNode];
	// Each side's leaders, small, under its record (points, rebounds,
	// assists).
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
							{/* (Its leaders by its record, under its name - in the room the
							    name leaves there, not under the record pushing it up.) */}
							{subs?.[i] || leaders?.[i] ? (
								<div className="mb-sub-wrap">
									{subs?.[i] ? <div className="tb-sub">{subs[i]}</div> : null}
									{leaders?.[i] ? (
										<div className="mb-leaders">{leaders[i]}</div>
									) : null}
								</div>
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
					{navs?.[i] ? (
						<div className="mb-nav">{navs[i]}</div>
					) : (
						<Stripes className="tb-stripes-bottom" />
					)}
				</div>
			);
		})}
		{tag || status || extra ? (
			<div className="mb-middle">
				{status ? <div className="mb-status">{status}</div> : null}
				{/* (Under the status: the day, the series.) */}
				{tag ? <div className="mb-tag">{tag}</div> : null}
				{extra ? <div className="mb-extra">{extra}</div> : null}
			</div>
		) : null}
	</div>
);

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
				asked = toWorker("main", "getAllTeamColorsUI", undefined).then(
					(x) => x,
				);
				allColorsAsked.set(lid, asked);
			}
			const got = await asked;
			if (!got) {
				return;
			}
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
