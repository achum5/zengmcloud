import type { RatingKey } from "../../../common/types.hockey.ts";
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
					<span style={gradient(ratings.endu)}>End: {ratings.endu}</span>
				</div>
				<div className="col-4">
					<span style={gradient(ratings.ovr)}>Ovr: {ratings.ovr}</span>
					<br />
					<span style={gradient(ratings.oiq)}>oIQ: {ratings.oiq}</span>
					<br />
					<span style={gradient(ratings.pss)}>Pss: {ratings.pss}</span>
					<br />
					<span style={gradient(ratings.wst)}>Wst: {ratings.wst}</span>
					<br />
					<span style={gradient(ratings.sst)}>Sst: {ratings.sst}</span>
					<br />
					<span style={gradient(ratings.stk)}>Stk: {ratings.stk}</span>
				</div>
				<div className="col-4">
					<span style={gradient(ratings.pot)}>
						Pot: {Math.round(ratings.pot)}
					</span>
					<br />
					<span style={gradient(ratings.diq)}>dIQ: {ratings.diq}</span>
					<br />
					<span style={gradient(ratings.chk)}>Chk: {ratings.chk}</span>
					<br />
					<span style={gradient(ratings.blk)}>Blk: {ratings.blk}</span>
					<br />
					<span style={gradient(ratings.fcf)}>Fcf: {ratings.fcf}</span>
					<br />
					<span style={gradient(ratings.glk)}>Glk: {ratings.glk}</span>
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
	if (stats && stats.keyStatsWithGoalieGP !== "") {
		statsBlock = (
			<div
				style={{
					whiteSpace: "normal",
				}}
			>
				<div className="fw-bold mb-1">{seasonPrefix2}Stats</div>
				{stats.keyStatsWithGoalieGP}
			</div>
		);
	} else {
		statsBlock = null;
	}

	return (
		<>
			{ratingsBlock}
			{statsBlock}
		</>
	);
};

export default RatingsStats;
