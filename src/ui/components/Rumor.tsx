import { helpers } from "../util/helpers.ts";
import type { EventBBGM } from "../../common/types.ts";

// THE RUMOR MILL'S LOOK (smartCpu/rumors.ts; owner, 2026-11-27: who's after
// whom, no heat words, "steer away from an ai vibe code look"). A rumor's card
// says who is after whom in the header, abbreviations beside the logos like
// the scores bar.

type Rumor = NonNullable<EventBBGM["rumor"]>;

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
