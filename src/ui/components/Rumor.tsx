import clsx from "clsx";
import { helpers } from "../util/helpers.ts";
import type { EventBBGM } from "../../common/types.ts";

// THE RUMOR MILL'S LOOK (smartCpu/rumors.ts; owner, 2026-11-27: heat tags and
// who's after whom, "steer away from an ai vibe code look"). A rumor says how
// far the talk got the way a newsroom kicker does - a short word above the
// story, its color by how hot it is - and its card says who is after whom in
// the header, abbreviations beside the logos like the scores bar.

type Rumor = NonNullable<EventBBGM["rumor"]>;

// Class names spelled out whole: the live CSS keeps only those it finds here.
const HEAT: Record<string, { text: string; className: string }> = {
	close: { text: "Close", className: "rumor-heat-close" },
	near: { text: "Offer made", className: "rumor-heat-near" },
	available: { text: "Available", className: "rumor-heat-available" },
	interest: { text: "Interest", className: "text-body-secondary" },
	team: { text: "Calling around", className: "text-body-secondary" },
	apart: { text: "Long shot", className: "text-body-tertiary" },
};

const heatOf = (rumor: Rumor) =>
	HEAT[rumor.kind ?? ""] ??
	(rumor.tier >= 3
		? HEAT.close!
		: rumor.tier === 2
			? HEAT.near!
			: HEAT.interest!);

export const RumorHeat = ({
	rumor,
	className,
}: {
	rumor: Rumor;
	className?: string;
}) => {
	const heat = heatOf(rumor);
	return (
		<span className={clsx("rumor-heat", heat.className, className)}>
			{heat.text}
		</span>
	);
};

type TeamInfo = {
	abbrev: string;
	imgURL?: string;
	imgURLSmall?: string;
};

const logoStyle = { maxHeight: 20, maxWidth: 20 };

const Team = ({
	tid,
	teams,
	season,
}: {
	tid: number;
	teams: Record<number, TeamInfo | undefined> | TeamInfo[];
	season: number;
}) => {
	const t = (teams as Record<number, TeamInfo | undefined>)[tid];
	if (!t) {
		return null;
	}
	const logo = t.imgURLSmall ?? t.imgURL;
	return (
		<a
			href={helpers.leagueUrl(["roster", `${t.abbrev}_${tid}`, season])}
			className="d-inline-flex align-items-center"
		>
			{logo ? (
				<img
					src={logo}
					alt=""
					style={logoStyle}
					className="me-1"
					// A logo that won't load leaves the abbreviation alone.
					onError={(event) => {
						event.currentTarget.style.display = "none";
					}}
				/>
			) : null}
			{t.abbrev}
		</a>
	);
};

// "MIL, ORL → POR": the teams after him, then his own.
export const RumorTeams = ({
	tids,
	rumor,
	teams,
	season,
}: {
	tids: readonly number[];
	rumor: Rumor;
	teams: Record<number, TeamInfo | undefined> | TeamInfo[];
	season: number;
}) => {
	const seller = rumor.seller;
	const after = tids.filter((tid) => tid !== seller);
	return (
		<span className="d-inline-flex align-items-center text-nowrap ps-2 fw-bold">
			{after.map((tid, i) => (
				<span key={tid} className="d-inline-flex align-items-center">
					{i > 0 ? <span className="me-1">,</span> : null}
					<Team tid={tid} teams={teams} season={season} />
				</span>
			))}
			{seller !== undefined ? (
				<>
					<span className="text-body-secondary fw-normal mx-1">→</span>
					<Team tid={seller} teams={teams} season={season} />
				</>
			) : null}
		</span>
	);
};
