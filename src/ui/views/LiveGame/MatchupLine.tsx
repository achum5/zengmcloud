import { TeamLogoInline } from "../../components/TeamLogoInline.tsx";
import { NationalTag } from "../../components/NationalTag.tsx";
import { helpers } from "../../util/helpers.ts";
import {
	matchupFacts,
	type MatchupBoxScore,
	type MatchupTeam as Team,
} from "./matchupFacts.ts";

const MatchupTeam = ({
	t,
	seed,
	showRecord,
}: {
	t: Team;
	seed?: number;
	showRecord: boolean;
}) => (
	<span className="live-matchup-team">
		<TeamLogoInline
			className="live-matchup-logo"
			imgURL={t.imgURL}
			imgURLSmall={t.imgURLSmall}
			alt={t.abbrev}
			size={28}
		/>
		{seed !== undefined ? (
			<span className="live-matchup-seed">{seed}</span>
		) : null}
		<span className="live-matchup-name">
			<span className="d-md-none">{t.abbrev}</span>
			<span className="d-none d-md-inline">
				{[t.region, t.name].filter(Boolean).join(" ") || t.abbrev}
			</span>
		</span>
		{showRecord && t.won !== undefined && t.lost !== undefined ? (
			<span className="live-matchup-record">
				{helpers.formatRecord({
					won: t.won,
					lost: t.lost,
					tied: t.tied,
					otl: t.otl,
				})}
			</span>
		) : null}
	</span>
);

// THE LIVE GAME'S HEADER, in place of the title bar: the two teams as they
// came in - their records (a playoff game: their seeds) - and the night's
// facts under them: the line, the total, the series, and the network if it's
// on one.
export const MatchupLine = ({ boxScore }: { boxScore: MatchupBoxScore }) => {
	const [home, away] = boxScore.teams;
	const real = home.tid >= 0 && away.tid >= 0 && !boxScore.exhibition;
	const playIn = boxScore.gamecast?.playIn;
	const seedOf = (t: Team) =>
		boxScore.playoffs ? (playIn?.seeds[t.tid] ?? t.playoffs?.seed) : undefined;
	const facts = real ? matchupFacts(boxScore) : [];
	// (The teams and the facts break onto two lines when they don't fit on
	// one; the network stays at the right of them either way.)
	return (
		<div className="live-matchup">
			<div className="live-matchup-main">
				<h1 className="live-matchup-teams">
					<MatchupTeam
						t={away}
						seed={seedOf(away)}
						showRecord={real && !boxScore.playoffs}
					/>
					<span className="live-matchup-at">@</span>
					<MatchupTeam
						t={home}
						seed={seedOf(home)}
						showRecord={real && !boxScore.playoffs}
					/>
				</h1>
				{facts.length > 0 ? (
					<div className="live-matchup-facts">
						{facts.map((fact, i) => (
							<span key={i} className="live-matchup-fact">
								{fact}
							</span>
						))}
					</div>
				) : null}
			</div>
			{boxScore.national ? (
				<NationalTag
					className="live-matchup-national"
					tag={boxScore.national}
				/>
			) : null}
		</div>
	);
};
