import useLocalStorageState from "use-local-storage-state";
import { helpers } from "../../util/helpers.ts";
import { formatAmerican } from "../../../common/sportsbook.ts";
import type { View } from "../../../common/types.ts";

// Basketball Reference's "More Team Info": a season's numbers with where each
// ranks in the league. Open or shut stays the way the user last left it.

type Info = NonNullable<View<"roster">["moreInfo"]>;
type Ranked = { value: number; rank: number };

const Stat = ({
	label,
	title,
	numTeams,
	ranked,
	format,
}: {
	label: string;
	title?: string;
	numTeams: number;
	ranked: Ranked | undefined;
	format: (value: number) => string;
}) =>
	ranked ? (
		<span className="text-nowrap">
			<span className="fw-bold" title={title}>
				{label}
			</span>
			: {format(ranked.value)}{" "}
			<span className="text-body-secondary">
				({helpers.ordinal(ranked.rank)} of {numTeams})
			</span>
		</span>
	) : null;

// A line of stats breaks between them on a narrow screen (each one stays
// together), rather than running off the side and widening the page.
const STAT_ROW = "d-flex flex-wrap column-gap-3";

const signed = (value: number, digits: number) =>
	`${value > 0 ? "+" : ""}${value.toFixed(digits)}`;

export const MoreTeamInfo = ({
	info,
	season,
}: {
	info: Info | undefined;
	season: number;
}) => {
	const [open, setOpen] = useLocalStorageState("rosterMoreTeamInfo", {
		defaultValue: false,
	});

	if (!info) {
		return null;
	}

	const { numTeams, preseasonOdds } = info;
	const hasOdds =
		preseasonOdds?.title !== undefined || preseasonOdds?.winTotal !== undefined;
	if (info.gp === 0 && !hasOdds && info.playoffs.length === 0) {
		return null;
	}

	return (
		<div className="mt-2">
			<button
				type="button"
				className="btn btn-light-bordered btn-sm"
				aria-expanded={open}
				onClick={() => {
					setOpen(!open);
				}}
			>
				More team info {open ? "▴" : "▾"}
			</button>
			{open ? (
				<div className="mt-2" style={{ maxWidth: 560 }}>
					{info.gp > 0 ? (
						<>
							<div className={STAT_ROW}>
								<Stat
									label="PTS/G"
									numTeams={numTeams}
									ranked={info.pts}
									format={(v) => v.toFixed(1)}
								/>
								<Stat
									label="Opp PTS/G"
									numTeams={numTeams}
									ranked={info.oppPts}
									format={(v) => v.toFixed(1)}
								/>
							</div>
							<div className={STAT_ROW}>
								<Stat
									label="SRS"
									title="Simple Rating System: margin of victory adjusted for strength of schedule"
									numTeams={numTeams}
									ranked={info.srs}
									format={(v) => signed(v, 2)}
								/>
								<Stat
									label="Pace"
									title="Possessions per 48 minutes"
									numTeams={numTeams}
									ranked={info.pace}
									format={(v) => v.toFixed(1)}
								/>
							</div>
							<div className={STAT_ROW}>
								<Stat
									label="Off Rtg"
									title="Points scored per 100 possessions"
									numTeams={numTeams}
									ranked={info.ortg}
									format={(v) => v.toFixed(1)}
								/>
								<Stat
									label="Def Rtg"
									title="Points allowed per 100 possessions"
									numTeams={numTeams}
									ranked={info.drtg}
									format={(v) => v.toFixed(1)}
								/>
								<Stat
									label="Net Rtg"
									numTeams={numTeams}
									ranked={info.nrtg}
									format={(v) => signed(v, 1)}
								/>
							</div>
							{info.expected ? (
								<div className={STAT_ROW}>
									<Stat
										label="Expected W-L"
										title="Pythagorean record, from points scored and allowed"
										numTeams={numTeams}
										ranked={info.expected}
										format={() =>
											`${info.expected!.won}-${info.expected!.lost}`
										}
									/>
								</div>
							) : null}
						</>
					) : null}
					{hasOdds ? (
						<div>
							<span className="fw-bold">Preseason Odds</span>:{" "}
							{[
								preseasonOdds!.title !== undefined
									? `Championship ${formatAmerican(preseasonOdds!.title)}`
									: undefined,
								preseasonOdds!.winTotal !== undefined
									? `Over-Under ${preseasonOdds!.winTotal}${preseasonOdds!.result ? ` (${preseasonOdds!.result})` : ""}`
									: undefined,
							]
								.filter(Boolean)
								.join(", ")}
						</div>
					) : null}
					{info.att ? (
						<div className={STAT_ROW}>
							<Stat
								label="Attendance"
								numTeams={numTeams}
								ranked={info.att}
								format={(v) =>
									`${helpers.numberWithCommas(Math.round(v))} per game`
								}
							/>
						</div>
					) : null}
					{info.playoffs.length > 0 ? (
						<div className="mt-1">
							<span className="fw-bold">{season} Playoffs</span>:
							{info.playoffs.map((series, i) => (
								<div key={i}>
									{helpers.upperCaseFirstLetter(series.result)} {series.round} (
									{series.won}-{series.lost}) versus{" "}
									<a
										href={helpers.leagueUrl([
											"roster",
											`${series.opp.abbrev}_${series.opp.tid}`,
											season,
										])}
									>
										{series.opp.region} {series.opp.name}
									</a>
								</div>
							))}
						</div>
					) : null}
				</div>
			) : null}
		</div>
	);
};
