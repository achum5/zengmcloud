import { DataTable } from "../../components/DataTable/index.tsx";
import Injuries from "./Injuries.tsx";
import useTitleBar from "../../hooks/useTitleBar.tsx";
import { helpers } from "../../util/helpers.ts";
import { getCols } from "../../../common/getCols.ts";
import type { View } from "../../../common/types.ts";
import { SeasonIcons } from "../../components/SeasonIcons.tsx";
import TopStuff from "./TopStuff.tsx";
import { PLAYER } from "../../../common/constants.ts";
import HideableSection from "../../components/HideableSection.tsx";
import { StatsTable } from "./StatsTable.tsx";
import { usePlayerTeamStats } from "./usePlayerTeamStats.ts";
import { highlightLeaderText, MaybeBold, SeasonLink } from "./common.tsx";
import { wrappedTeamAbbrevLink } from "../../components/TeamAbbrevLink.tsx";
import { wrappedCurrency } from "../../components/wrappedCurrency.ts";
import { groupAwards } from "../../util/groupAwards.ts";
import { InjuryIcon } from "../../components/InjuryIcon.tsx";
import { SkillsBlock } from "../../components/SkillsBlock.tsx";
import { SafeHtml } from "../../components/SafeHtml.tsx";
import { SeasonNoteButton } from "../../components/SeasonNoteButton.tsx";
import { useLocal } from "../../util/local.ts";
import { splitPlayerNote } from "../../../common/seasonNote.ts";
import { buildPlayerNoteLinks } from "../../util/linkifyRecap.ts";
import { TradingCardGallery } from "../../components/TradingCardGallery.tsx";

const Player2 = ({
	bestPos,
	customMenu,
	events,
	feats,
	jerseyNumberInfos,
	leaders,
	noteTeammates,
	player,
	randomDebutsForeverPids,
	ratings,
	retired,
	statTables,
	statSummary,
	teamColors,
	teamJersey,
	teamName,
	teamURL,
	tradingCards,
	willingToSign,
}: View<"player">) => {
	const {
		challengeNoRatings,
		season: currentSeason,
		teamInfoCache,
	} = useLocal(["challengeNoRatings", "season", "teamInfoCache"]);
	const showRatings = !challengeNoRatings || retired;

	// Still a prospect: the draft he belongs to hasn't happened yet.
	const undrafted = player.tid === PLAYER.UNDRAFTED;

	// Every piece of a player's writeup hangs off the thing it is about: the
	// draft recap on his draft line, the scouting report on his draft season's
	// ratings row, each season recap on that season's row. Only hand-written
	// text is left for the note block - except on a prospect, whose scouting
	// report IS his page and stays at the top.
	const seasonNotes = splitPlayerNote(player.note, {
		draftYear: player.draft.year,
		undrafted,
		seasonsWithStats: new Set(
			player.stats.map((ps: { season: number }) => ps.season),
		),
		seasonsWithRatings: new Set(
			player.ratings.map((r: { season: number }) => r.season),
		),
	});
	const noteLinksBySeason = buildPlayerNoteLinks(teamInfoCache, noteTeammates);

	// What hangs off a ratings row: the scouting report on the draft season
	// (it was filed a year early, under a season he has no row for at all), and
	// any writeup for a year he was on a roster but never played.
	const ratingsNotes = new Map(seasonNotes.byRatingsSeason);
	if (seasonNotes.scouting.length > 0 && player.draft.year !== undefined) {
		ratingsNotes.set(player.draft.year, [
			...seasonNotes.scouting,
			...(ratingsNotes.get(player.draft.year) ?? []),
		]);
	}

	// Per-team career totals (bref-style team rows), fetched once and shared by
	// every stat table below.
	const teamStats = usePlayerTeamStats(player.pid, player.stats.length);

	useTitleBar({
		title: player.name,
		customMenu,
		dropdownView: "player",
		dropdownFields:
			player.tid !== PLAYER.UNDRAFTED
				? {
						playerProfile: "overview",
					}
				: undefined,
		dropdownCustomURL: (fields) => {
			let gameLogSeason;
			if (player.stats.length > 0) {
				gameLogSeason = player.stats.at(-1)!.season;
			} else if (player.ratings.length > 0) {
				gameLogSeason = player.ratings.at(-1)!.season;
			} else {
				gameLogSeason = currentSeason;
			}

			const parts =
				fields.playerProfile === "gameLog"
					? ["player_game_log", player.pid, gameLogSeason]
					: ["player", player.pid];

			return helpers.leagueUrl(parts);
		},
	});

	const awardsGrouped = groupAwards(player.awards);

	let hasLeader = false;
	for (const row of Object.values(leaders)) {
		if (row && (row.attrs.has("age") || row.ratings.size > 0)) {
			hasLeader = true;
			break;
		}
	}

	return (
		<>
			<TopStuff
				bestPos={bestPos}
				currentSeason={currentSeason}
				jerseyNumberInfos={jerseyNumberInfos}
				noteTeammates={noteTeammates}
				// Only what has nowhere else to go; every writeup is on the row it is
				// about. On a prospect that is the whole note, scouting and all.
				displayNote={seasonNotes.leftover}
				// A prospect's note IS his scouting report, filed under the season
				// before his draft. That year is noise on a page about a player who
				// hasn't played yet, and reads as if it were his draft year. The
				// header is still parsed (it scopes the writeup's links); only the
				// "[YYYY]" label is dropped at render.
				hideSeasonLabels={undrafted}
				draftRecap={seasonNotes.draftRecap}
				noteLinksBySeason={noteLinksBySeason}
				player={player}
				randomDebutsForeverPids={randomDebutsForeverPids}
				retired={retired}
				showRatings={showRatings}
				statSummary={statSummary}
				teamColors={teamColors}
				teamJersey={teamJersey}
				teamName={teamName}
				teamURL={teamURL}
				willingToSign={willingToSign}
			/>

			{statTables.map(({ name, onlyShowIf, stats, superCols }, i) => (
				<StatsTable
					key={name}
					name={name}
					onlyShowIf={onlyShowIf}
					stats={stats}
					superCols={superCols}
					p={player}
					leaders={leaders}
					teamStats={teamStats}
					// Only the first table gets the writeup arrows. Every table lists
					// the same seasons, so repeating them five times down the page
					// would be five ways to open the same popover.
					seasonNotes={i === 0 ? seasonNotes.bySeason : undefined}
					noteLinksBySeason={i === 0 ? noteLinksBySeason : undefined}
				/>
			))}

			<HideableSection
				title="Ratings"
				description={hasLeader && showRatings ? highlightLeaderText : null}
			>
				<DataTable
					className="mb-3 datatable-negative-margin-top"
					cols={getCols([
						"Year",
						"Team",
						"Age",
						"Pos",
						"Ovr",
						"Pot",
						...ratings.map((rating) => `rating:${rating}`),
						"Skills",
					])}
					defaultSort={[0, "asc"]}
					defaultStickyCols={2}
					hideAllControls
					name="Player:Ratings"
					rows={player.ratings.map((r, i) => {
						return {
							key: i,
							data: [
								{
									searchValue: r.season,
									sortValue: i,
									value: (
										<>
											<SeasonLink pid={player.pid} season={r.season} />{" "}
											<SeasonIcons season={r.season} awards={player.awards} />
											{r.injuryIndex !== undefined &&
											player.injuries[r.injuryIndex] ? (
												<InjuryIcon
													injury={{
														type: player.injuries[r.injuryIndex]!.type,
														gamesRemaining: -1,
													}}
												/>
											) : null}
											{ratingsNotes.get(r.season) ? (
												<SeasonNoteButton
													header={
														r.season === player.draft.year
															? `Scouting report — ${player.draft.year} draft`
															: r.season
													}
													id={`ratings-note-${r.season}`}
													linksFor={noteLinksBySeason}
													sections={ratingsNotes.get(r.season)!}
													title={
														r.season === player.draft.year
															? "Read the scouting report"
															: `Read the ${r.season} writeup`
													}
												/>
											) : null}
										</>
									),
								},
								wrappedTeamAbbrevLink({
									abbrev: r.abbrev,
									season: r.season,
									tid: r.tid,
								}),
								<MaybeBold bold={leaders[r.season]?.attrs.has("age")}>
									{r.age}
								</MaybeBold>,
								r.pos,
								showRatings ? (
									<MaybeBold bold={leaders[r.season]?.ratings.has("ovr")}>
										{r.ovr}
									</MaybeBold>
								) : null,
								showRatings ? (
									<MaybeBold bold={leaders[r.season]?.ratings.has("pot")}>
										{r.pot}
									</MaybeBold>
								) : null,
								...ratings.map((rating) =>
									showRatings ? (
										<MaybeBold bold={leaders[r.season]?.ratings.has(rating)}>
											{(r as any)[rating]}
										</MaybeBold>
									) : null,
								),
								<SkillsBlock className="skills-alone" skills={r.skills} />,
							],
						};
					})}
				/>
			</HideableSection>

			<div className="row">
				<div className="col-6 col-md-3">
					<HideableSection title="Awards">
						{awardsGrouped.length > 0 ? (
							<table className="table table-nonfluid table-striped table-borderless table-sm player-awards">
								<tbody>
									{awardsGrouped.map((a, i) => {
										return (
											<tr key={i}>
												<td>
													{a.count > 1 ? `${a.count}x ` : null}
													{a.type} ({a.seasons.join(", ")})
												</td>
											</tr>
										);
									})}
								</tbody>
							</table>
						) : null}
						{awardsGrouped.length === 0 ? <p>None</p> : null}
					</HideableSection>
				</div>
				<div className="col-6 col-md-3">
					<HideableSection title="Salaries">
						<DataTable
							className="datatable-negative-margin-top mb-3"
							cols={getCols(["Year", "Amount"])}
							defaultSort={[0, "asc"]}
							footer={{
								data: [
									"Total",
									helpers.formatCurrency(player.salariesTotal, "M"),
								],
							}}
							hideAllControls
							name="Player:Salaries"
							rows={player.salaries.map((s, i) => {
								return {
									key: i,
									data: [
										{
											searchValue: s.season,
											sortValue: i,
											value: (
												<>
													<SeasonLink pid={player.pid} season={s.season} />{" "}
													<SeasonIcons
														season={s.season}
														awards={player.awards}
													/>
												</>
											),
										},
										wrappedCurrency(s.amount, "M"),
									],
									classNames:
										s.type === "current"
											? "fw-bold"
											: s.type === "future"
												? "fst-italic"
												: undefined,
								};
							})}
						/>
					</HideableSection>
				</div>
				<div className="col-md-6">
					<HideableSection title="Statistical Feats">
						<div
							className="small-scrollbar"
							style={{
								maxHeight: 500,
								overflowY: "auto",
							}}
						>
							{feats.map((e) => {
								return (
									<p key={e.eid}>
										<b>{e.season}</b>: <SafeHtml dirty={e.text} />
									</p>
								);
							})}
						</div>
						{feats.length === 0 ? <p>None</p> : null}
					</HideableSection>
				</div>
			</div>

			<div className="row">
				<div className="col-md-6 col-lg-4">
					<HideableSection title="Injuries">
						<Injuries injuries={player.injuries} showRatings={showRatings} />
					</HideableSection>
				</div>
				<div className="col-md-6 col-lg-8">
					<HideableSection title="Transactions">
						{events.map((e) => {
							return (
								<p key={e.eid}>
									<b>{e.season}</b>: <SafeHtml dirty={e.text} />
								</p>
							);
						})}
						{events.length === 0 ? <p>None</p> : null}
					</HideableSection>
				</div>
			</div>

			<div className="row" style={{ marginBottom: "-1rem" }}>
				<div className="col">
					<HideableSection title="Cards">
						<TradingCardGallery cards={tradingCards} />
					</HideableSection>
				</div>
			</div>
		</>
	);
};

export default Player2;
