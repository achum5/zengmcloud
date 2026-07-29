import { useState } from "react";
import DraftClass from "./DraftClass.tsx";
import useTitleBar from "../../hooks/useTitleBar.tsx";
import type { View } from "../../../common/types.ts";
import { MoreLinks } from "../../components/MoreLinks.tsx";
import { PlayerRecaps } from "../../components/PlayerRecaps.tsx";
import { useLocal } from "../../util/local.ts";

const PAGE_SIZE = 3;

const DraftScouting = ({ fantasyDraft, seasons }: View<"draftScouting">) => {
	const { challengeNoRatings, draftType, godMode, season } = useLocal([
		"challengeNoRatings",
		"draftType",
		"godMode",
		"season",
	]);

	const noDraft = draftType === "freeAgents";

	useTitleBar({ title: !noDraft ? "Draft Scouting" : "Upcoming Prospects" });

	const [page, setPage] = useState(0);

	if (seasons.length <= PAGE_SIZE && page !== 0) {
		setPage(0);
	}

	const pagination = seasons.length > PAGE_SIZE;
	const maxPage = Math.ceil(seasons.length / PAGE_SIZE) - 1;
	const enablePrevious = pagination && page > 0;
	const enableNext = pagination && page < maxPage;

	let seasonsToDisplay;
	if (pagination) {
		const indexStart = page * PAGE_SIZE;
		const indexEnd = indexStart + PAGE_SIZE;
		seasonsToDisplay = seasons.slice(indexStart, indexEnd);
	} else {
		seasonsToDisplay = seasons;
	}

	return (
		<>
			<MoreLinks type="draft" page="draft_scouting" draftType={draftType} />

			<p>
				The ratings shown are your scouts' projections for what the players'
				ratings will be when they enter the {!noDraft ? "draft" : "league"}. The
				further in the future, the more uncertainty there is in their estimates.
			</p>

			{/* Only the class one year out, filed under the current season. The
			    further-out classes on this page are still churning as scouting
			    improves, so a report written on one now would be about a player
			    who no longer exists. Removes itself once the class is written. */}
			<div className="mb-3">
				<PlayerRecaps
					season={season}
					filter="prospects"
					heading="Draft Prospect Reports (AI)"
				/>
			</div>

			{pagination ? (
				<div className="d-flex flex-row-reverse">
					<div className="btn-group">
						<button
							className="btn btn-light-bordered"
							disabled={!enablePrevious}
							onClick={() => {
								setPage((page) => page - 1);
							}}
						>
							Previous
						</button>
						<button
							className="btn btn-light-bordered"
							disabled={!enableNext}
							onClick={() => {
								setPage((page) => page + 1);
							}}
						>
							Next
						</button>
					</div>
				</div>
			) : null}

			<div className="row">
				{seasonsToDisplay.map((info, offset) => {
					return (
						<div key={info.season} className="col-md-4 col-sm-6">
							<DraftClass
								challengeNoRatings={challengeNoRatings}
								fantasyDraft={fantasyDraft}
								godMode={godMode}
								offset={offset}
								players={info.players}
								season={info.season}
							/>
						</div>
					);
				})}
			</div>
		</>
	);
};

export default DraftScouting;
