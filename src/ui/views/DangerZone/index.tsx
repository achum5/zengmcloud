import { useState } from "react";
import { PHASE, WEBSITE_ROOT } from "../../../common/constants.ts";
import type { FaceAgingScope } from "../../../worker/core/player/applyFaceAgingToLeague.ts";
import type { View } from "../../../common/types.ts";
import useTitleBar from "../../hooks/useTitleBar.tsx";
import { confirm } from "../../util/confirm.tsx";
import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";
import { showNotification } from "../../util/showNotification.ts";
import { toWorker } from "../../util/toWorker.ts";
import AutoSave from "./AutoSave.tsx";
import WorkerConsole from "./WorkerConsole.tsx";

const DangerZone = ({ autoSave }: View<"dangerZone">) => {
	useTitleBar({
		title: "Danger Zone",
	});

	const { godMode, phase } = useLocal(["godMode", "phase"]);

	const [faceAgingScope, setFaceAgingScope] = useState<FaceAgingScope>("all");
	const [agingFaces, setAgingFaces] = useState(false);
	const [clearingRecaps, setClearingRecaps] = useState(false);
	const [backfillingRanks, setBackfillingRanks] = useState(false);

	return (
		<>
			{!godMode ? (
				<div>
					<span className="alert alert-warning d-inline-block">
						These features are only available in{" "}
						<a href={helpers.leagueUrl(["god_mode"])}>God Mode</a>.
					</span>
				</div>
			) : null}
			<div className="row">
				<div className="col-md-6">
					<h2>Skip to...</h2>

					<p className="alert alert-danger">
						<b>Warning!</b> Skipping ahead might break your league! It's only
						here in case your league is already broken, in which case sometimes
						these drastic measures might save it.
					</p>

					<div className="btn-group mb-5">
						<button
							type="button"
							className="btn btn-light-bordered"
							disabled={!godMode}
							onClick={() => {
								toWorker("toolsMenu", "skipToPlayoffs", undefined);
							}}
						>
							Playoffs
						</button>
						<button
							type="button"
							className="btn btn-light-bordered"
							disabled={!godMode}
							onClick={() => {
								toWorker("toolsMenu", "skipToBeforeDraft", undefined);
							}}
						>
							Before draft
						</button>
						<button
							type="button"
							className="btn btn-light-bordered"
							disabled={!godMode}
							onClick={() => {
								toWorker("toolsMenu", "skipToAfterDraft", undefined);
							}}
						>
							After draft
						</button>
						<button
							type="button"
							className="btn btn-light-bordered"
							disabled={!godMode}
							onClick={() => {
								toWorker("toolsMenu", "skipToPreseason", undefined);
							}}
						>
							Preseason
						</button>
					</div>

					<h2>Trade deadline</h2>

					<p>
						This will not sim any games, it will just toggle whether the trade
						deadline has passed or not this season, and delete any scheduled
						trade deadline later this season.
					</p>

					{phase !== PHASE.REGULAR_SEASON &&
					phase !== PHASE.AFTER_TRADE_DEADLINE ? (
						<p className="text-warning">
							This only works during the regular season.
						</p>
					) : null}

					{phase === PHASE.AFTER_TRADE_DEADLINE ? (
						<button
							type="button"
							className="btn btn-god-mode border-0"
							disabled={!godMode}
							onClick={() => {
								toWorker("main", "toggleTradeDeadline", undefined);
							}}
						>
							Switch to before trade deadline
						</button>
					) : (
						<button
							type="button"
							className="btn btn-god-mode border-0"
							disabled={phase !== PHASE.REGULAR_SEASON || !godMode}
							onClick={() => {
								toWorker("main", "toggleTradeDeadline", undefined);
							}}
						>
							Switch to after trade deadline
						</button>
					)}

					<div className="mt-5">
						<h2>All-Star Game</h2>

						<p>
							If the All-Star Game has not yet happened, you can move it up to
							right now, so that it will happen before the next currently
							scheduled game. This also works if the current season has no
							All-Star Game - it will add one, and it will happen before the
							next game.
						</p>

						<p>
							If the All-Star Game has already happened and you add another
							one... I guess you get an extra All-Star Game?
						</p>

						{phase !== PHASE.REGULAR_SEASON &&
						phase !== PHASE.AFTER_TRADE_DEADLINE ? (
							<p className="text-warning">
								This only works during the regular season.
							</p>
						) : null}

						<button
							type="button"
							className="btn btn-god-mode border-0"
							disabled={
								(phase !== PHASE.REGULAR_SEASON &&
									phase !== PHASE.AFTER_TRADE_DEADLINE) ||
								!godMode
							}
							onClick={async () => {
								await toWorker("main", "allStarGameNow", undefined);

								showNotification({
									text: "The All-Star Game has been scheduled.",
									type: "info",
								});
							}}
						>
							Schedule All-Star Game now
						</button>
					</div>
				</div>

				<div className="col-md-6 mt-5 mt-sm-0">
					<h2>Face aging</h2>

					<p>
						Age every existing player's face to match how old he is now,
						including the seasons in between. Faces normally only age going
						forward, so a league that has already been running keeps players who
						look however old they were when they were created.
					</p>

					<div className="d-flex flex-wrap gap-2 mb-5">
						<select
							className="form-select"
							onChange={(event) => {
								setFaceAgingScope(event.target.value as FaceAgingScope);
							}}
							style={{ maxWidth: "170px" }}
							value={faceAgingScope}
						>
							<option value="all">All players</option>
							<option value="fictional">Fictional players only</option>
							<option value="real">Real players only</option>
						</select>
						<button
							type="button"
							className="btn btn-light-bordered"
							disabled={agingFaces}
							onClick={async () => {
								setAgingFaces(true);
								try {
									const count = await toWorker(
										"toolsMenu",
										"applyFaceAging",
										faceAgingScope,
									);
									showNotification({
										text: `Face aging applied to ${count} player${count === 1 ? "" : "s"}.`,
										type: "success",
									});
								} finally {
									setAgingFaces(false);
								}
							}}
						>
							{agingFaces ? "Working..." : "Apply aging"}
						</button>
					</div>

					<h2>Recaps</h2>

					<p>
						Automatic recaps always use the current generator, but a recap you
						filed with the AI or wrote yourself overrides it. This deletes every
						filed game and day recap in the league.
					</p>

					<button
						type="button"
						className="btn btn-light-bordered mb-5"
						disabled={clearingRecaps}
						onClick={async () => {
							const proceed = await confirm(
								"Delete every filed game and day recap in this league? The automatic recaps will show instead. This cannot be undone.",
								{
									title: "Delete filed recaps",
									// Opens with Cancel focused: this throws away text the
									// user wrote or paid an AI to write, and there is no
									// undo.
									danger: true,
									okText: "Delete",
								},
							);
							if (!proceed) {
								return;
							}

							setClearingRecaps(true);
							try {
								const { games, days } = await toWorker(
									"toolsMenu",
									"clearFiledRecaps",
									undefined,
								);
								showNotification({
									text:
										games === 0 && days === 0
											? "No filed recaps to delete - every box score was already using the automatic recap."
											: `Deleted ${games} game recap${games === 1 ? "" : "s"} and ${days} day recap${days === 1 ? "" : "s"}.`,
									type: "success",
								});
							} finally {
								setClearingRecaps(false);
							}
						}}
					>
						{clearingRecaps ? "Working..." : "Delete filed recaps"}
					</button>

					<h2>Awards</h2>

					<p>
						Seasons played before this update recorded only award winners, not
						the players behind them. This re-decides them with their own
						formulas and box scores, so a player page shows MVP-3 for a
						third-place finish. Winners never change.
					</p>

					<button
						type="button"
						className="btn btn-light-bordered mb-5"
						disabled={!godMode || backfillingRanks}
						onClick={async () => {
							const proceed = await confirm(
								"Fill in the players behind every past award winner? This adds to award histories across the league and cannot be undone.",
								{
									title: "Fill in award voting",
									okText: "Fill in",
								},
							);
							if (!proceed) {
								return;
							}

							setBackfillingRanks(true);
							try {
								const { seasons, ranks } = await toWorker(
									"toolsMenu",
									"backfillAwardVotingRanks",
									undefined,
								);
								showNotification({
									text:
										ranks === 0
											? "Nothing to fill in - every past award already lists the players behind the winner."
											: `Added ${ranks} placing${ranks === 1 ? "" : "s"} across ${seasons} season${seasons === 1 ? "" : "s"}.`,
									type: "success",
								});
							} finally {
								setBackfillingRanks(false);
							}
						}}
					>
						{backfillingRanks ? "Working..." : "Fill in award voting"}
					</button>

					<h2>Auto save</h2>

					<p>
						By default, your league is automatically saved as you play. Usually
						this is what you want. But sometimes you might want to experiment
						with re-playing parts of the game multiple times. When your league
						is saved automatically, you can't easily do that.
					</p>
					<p>
						To enable that kind of experimentation, here you can disable auto
						saving. This is not well tested and could break things, but it seems
						to generally work.
					</p>
					<p>
						If you play enough seasons with auto saving disabled, things will
						get slow because it has to keep everything in memory. But within a
						single season, disabling auto saving will actually make things
						faster.
					</p>

					<p>
						This setting is only temporary. If you restart your browser or
						switch to another league, auto save will be enabled again.
					</p>

					<AutoSave autoSave={autoSave} godMode={godMode} />

					<h2 className="mt-5">Worker console</h2>

					<p>
						If all the God Mode settings aren't enough for you, you can do more
						advanced customization by running some code that modifies your
						league.{" "}
						<a href={`https://${WEBSITE_ROOT}/manual/worker-console/`}>
							Click here for more information and some example code snippets.
						</a>
					</p>

					<p className="alert alert-danger">
						<b>Warning!</b> Please make sure the code you enter here comes from
						a trusted source. Malicious code could edit or delete any of your
						leagues.
					</p>

					<WorkerConsole godMode={godMode} />
				</div>
			</div>
		</>
	);
};

export default DangerZone;
