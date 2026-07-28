import { helpers } from "../../util/helpers.ts";
import type { RatingKey } from "../../../common/types.basketball.ts";
import { ratingsGradientStyle } from "./ratingsGradientStyle.ts";

type Props = {
	ratings?: {
		pos: string;
		ovr: number;
		pot: number;
	} & Record<RatingKey, number>;
	stats: any;
	type?: "career" | "current" | "draft" | number;
	challengeNoRatings: boolean;
	// Whether these particular ratings are the coarse, tens-digit kind.
	coarseRatings: boolean;
};

const RatingsStats = ({
	challengeNoRatings,
	coarseRatings,
	ratings,
	stats,
	type,
}: Props) => {
	const gradient = (rating: number) =>
		ratingsGradientStyle(rating, coarseRatings);
	const seasonPrefix =
		typeof type === "number" ? `${type} ` : type === "career" ? "Peak " : "";
	const seasonPrefix2 =
		type === "career" || type === "draft" ? "Career " : seasonPrefix;

	let ratingsBlock;

	if (challengeNoRatings) {
		ratingsBlock = null;
	} else if (ratings) {
		ratingsBlock = (
			<div className="row mb-2">
				<div className="col-4">
					<b>{seasonPrefix}Ratings</b>
					<br />
					<span style={gradient(ratings.hgt)}>Hgt: {ratings.hgt}</span>
					<br />
					<span style={gradient(ratings.stre)}>Str: {ratings.stre}</span>
					<br />
					<span style={gradient(ratings.spd)}>Spd: {ratings.spd}</span>
					<br />
					<span style={gradient(ratings.jmp)}>Jmp: {ratings.jmp}</span>
					<br />
					<span style={gradient(ratings.endu)}>End: {ratings.endu}</span>
				</div>
				<div className="col-4">
					<span style={gradient(ratings.ovr)}>Ovr: {ratings.ovr}</span>
					<br />
					<span style={gradient(ratings.ins)}>Ins: {ratings.ins}</span>
					<br />
					<span style={gradient(ratings.dnk)}>Dnk: {ratings.dnk}</span>
					<br />
					<span style={gradient(ratings.ft)}>Ft: {ratings.ft}</span>
					<br />
					<span style={gradient(ratings.fg)}>2Pt: {ratings.fg}</span>
					<br />
					<span style={gradient(ratings.tp)}>3Pt: {ratings.tp}</span>
				</div>
				<div className="col-4">
					<span style={gradient(ratings.pot)}>
						Pot: {Math.round(ratings.pot)}
					</span>
					<br />
					<span style={gradient(ratings.oiq)}>oIQ: {ratings.oiq}</span>
					<br />
					<span style={gradient(ratings.diq)}>dIQ: {ratings.diq}</span>
					<br />
					<span style={gradient(ratings.drb)}>Drb: {ratings.drb}</span>
					<br />
					<span style={gradient(ratings.pss)}>Pss: {ratings.pss}</span>
					<br />
					<span style={gradient(ratings.reb)}>Reb: {ratings.reb}</span>
				</div>
			</div>
		);
	} else {
		ratingsBlock = (
			<div className="row mb-2">
				<div className="col-12">
					<b>{seasonPrefix}Ratings</b>
					<br />
					<br />
					<br />
					<br />
					<br />
					<br />
					<br />
				</div>
			</div>
		);
	}

	let statsBlock;

	if (stats) {
		statsBlock = (
			<div className="row">
				<div className="col-4">
					<b>{seasonPrefix2}Stats</b>
					<br />
					PTS: {helpers.roundStat(stats.pts, "pts")}
					<br />
					TRB: {helpers.roundStat(stats.trb, "trb")}
					<br />
					AST: {helpers.roundStat(stats.ast, "ast")}
					<br />
					FG: {helpers.roundStat(stats.fgp, "fgp")}%
					<br />
					TS: {helpers.roundStat(stats.tsp, "tsp")}%
				</div>
				<div className="col-4">
					<br />
					BLK: {helpers.roundStat(stats.blk, "blk")}
					<br />
					STL: {helpers.roundStat(stats.stl, "stl")}
					<br />
					TO: {helpers.roundStat(stats.tov, "tov")}
					<br />
					3P: {helpers.roundStat(stats.tpp, "tpp")}%
					<br />
					3PAr: {helpers.roundStat(stats.tpar, "tpar")}
				</div>
				<div className="col-4">
					<br />
					MP: {helpers.roundStat(stats.min, "min")}
					<br />
					PER: {helpers.roundStat(stats.per, "per")}
					<br />
					EWA: {helpers.roundStat(stats.ewa, "ewa")}
					<br />
					FT: {helpers.roundStat(stats.ftp, "ftp")}%
					<br />
					FTr: {helpers.roundStat(stats.ftr, "ftr")}
				</div>
			</div>
		);
	} else {
		statsBlock = (
			<div className="row mt-2">
				<div className="col-12">
					<b>{seasonPrefix2}Stats</b>
					<br />
					<br />
					<br />
					<br />
				</div>
			</div>
		);
	}

	return (
		<>
			{ratingsBlock}
			{statsBlock}
		</>
	);
};

export default RatingsStats;
