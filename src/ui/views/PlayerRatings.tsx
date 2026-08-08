import { DataTable } from "../components/DataTable/index.tsx";
import { MoreLinks } from "../components/MoreLinks.tsx";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { getCols } from "../../common/getCols.ts";
import { POSITIONS, PLAYER } from "../../common/constants.ts";
import type { View } from "../../common/types.ts";
import {
	wrappedContractAmount,
	wrappedContractExp,
} from "../components/contract.tsx";
import { wrappedPlayerNameLabels } from "../components/PlayerNameLabels.tsx";
import { contractValueCell } from "../util/contractValueCell.tsx";
import type { DataTableRow } from "../components/DataTable/index.tsx";
import { bySport } from "../../common/sportFunctions.ts";
import { useLocal } from "../util/local.ts";
import { exemptFromCoarseRatings } from "../../common/coarsenRating.ts";

const PlayerRatings = ({
	abbrev,
	page,
	players,
	ratings,
	season,
}: View<"playerRatings">) => {
	useTitleBar({
		title: "Player Ratings",
		jumpTo: true,
		jumpToSeason: season,
		dropdownView: "player_ratings",
		dropdownFields: { teamsAndAllWatchPlayoffs: abbrev, seasons: season },
	});

	const {
		challengeNoRatings,
		hideRatingsOnesDigitExceptProspects,
		season: currentSeason,
		userTid,
	} = useLocal([
		"challengeNoRatings",
		"hideRatingsOnesDigitExceptProspects",
		"season",
		"userTid",
	]);

	const ovrsPotsColNames: string[] = [];
	if (
		bySport({
			baseball: true,
			basketball: false,
			football: true,
			hockey: true,
		})
	) {
		for (const pos of POSITIONS) {
			for (const type of ["ovr", "pot"]) {
				ovrsPotsColNames.push(`rating:${type}${pos}`);
			}
		}
	}

	const cols = getCols([
		"Name",
		"Pos",
		"Team",
		"Age",
		"Contract",
		"Exp",
		"Contract Value",
		"Ovr",
		"Pot",
		...ratings.map((rating) => `rating:${rating}`),
		...ovrsPotsColNames,
	]);

	const rows: DataTableRow[] = players.map((p) => {
		const showRatings = !challengeNoRatings || p.tid === PLAYER.RETIRED;

		const ovrsPotsRatings: string[] = [];
		if (
			bySport({
				baseball: true,
				basketball: false,
				football: true,
				hockey: true,
			})
		) {
			for (const pos of POSITIONS) {
				for (const type of ["ovrs", "pots"]) {
					ovrsPotsRatings.push(showRatings ? p.ratings[type][pos] : null);
				}
			}
		}

		const wrappedName = wrappedPlayerNameLabels({
			pid: p.pid,
			injury: p.injury,
			season,
			skills: p.ratings.skills,
			jerseyNumber: p.stats.jerseyNumber,
			defaultWatch: p.watch,
			firstName: p.firstName,
			firstNameShort: p.firstNameShort,
			lastName: p.lastName,
			awards: p.awards,
			awardsSeason: season,
		});

		return {
			key: p.pid,
			metadata: {
				type: "player",
				pid: p.pid,
				season,
				playoffs: "regularSeason",
			},
			// Undrafted prospects keep their exact ratings when the ones digit is
			// hidden, so this table mixes 78s with 7s. Sorting needs to know which
			// is which.
			coarseExempt: exemptFromCoarseRatings(
				p.tid,
				hideRatingsOnesDigitExceptProspects,
			),
			data: [
				wrappedName,
				p.ratings.pos,
				<a
					href={helpers.leagueUrl([
						"roster",
						`${p.stats.abbrev}_${p.stats.tid}`,
						season,
					])}
				>
					{p.stats.abbrev}
				</a>,
				p.age,
				p.contract.amount > 0 ? wrappedContractAmount(p) : null,
				p.contract.amount > 0 && season === currentSeason
					? wrappedContractExp(p)
					: null,
				contractValueCell(p.contractValue),
				showRatings ? p.ratings.ovr : null,
				showRatings ? p.ratings.pot : null,
				...ratings.map((rating) => (showRatings ? p.ratings[rating] : null)),
				...ovrsPotsRatings,
			],
			classNames: {
				"table-danger": p.hof,
				"table-info": p.stats.tid === userTid,
			},
		};
	});

	return (
		<>
			<MoreLinks type="playerRatings" page="player_ratings" season={season} />

			{challengeNoRatings ? (
				<p className="alert alert-danger d-inline-block">
					<b>Challenge Mode:</b> All player ratings are hidden, except for
					retired players.
				</p>
			) : null}

			<p>
				Players on your team are{" "}
				<span className="text-info">highlighted in blue</span>. Players in the
				Hall of Fame are <span className="text-danger">highlighted in red</span>
				.
			</p>

			<DataTable
				cols={cols}
				currentPage={page}
				defaultSort={[6, "desc"]}
				defaultStickyCols={window.mobile ? 0 : 1}
				name="PlayerRatings"
				pagination
				// The page is part of the address, so a particular page of the ratings
				// can be linked, bookmarked and opened in a new tab, and the back
				// button walks through them.
				pageUrl={(newPage) =>
					helpers.leagueUrl([
						"player_ratings",
						abbrev,
						season,
						...(newPage > 1 ? [newPage] : []),
					])
				}
				rows={rows}
			/>
		</>
	);
};

export default PlayerRatings;
