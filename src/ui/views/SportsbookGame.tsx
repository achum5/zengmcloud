import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import type { View } from "../../common/types.ts";
import { useLocal } from "../util/local.ts";
import { TeamLogoInline } from "../components/TeamLogoInline.tsx";
import {
	BetSlipCard,
	OddsCell,
	useBetSlip,
} from "../components/SportsbookBetSlip.tsx";

const TABLE_CLASS =
	"table table-striped table-borderless table-sm align-middle mb-3";

const STAT_LABELS: Record<string, string> = {
	pts: "Points",
	trb: "Rebounds",
	ast: "Assists",
	stl: "Steals",
	blk: "Blocks",
	tp: "3-Pointers Made",
	tov: "Turnovers",
	pra: "Pts + Reb + Ast",
	pr: "Pts + Reb",
	pa: "Pts + Ast",
};
const STAT_ORDER = ["pts", "trb", "ast", "stl", "blk", "tp", "tov", "pra", "pr", "pa"];

type GameBoard = NonNullable<View<"sportsbookGame">["board"]>;
type PlayerRow = GameBoard["home"]["players"][number];

const SportsbookGame = ({ gid, board, wallet, season }: View<"sportsbookGame">) => {
	useTitleBar({
		title: "Game Props",
	});

	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const slip = useBetSlip(wallet.tid);

	const Logo = ({ tid, size = 20 }: { tid: number; size?: number }) => {
		const t = teamInfoCache[tid];
		return (
			<TeamLogoInline
				imgURL={t?.imgURL}
				imgURLSmall={t?.imgURLSmall}
				size={size}
				includePlaceholderIfNoLogo
			/>
		);
	};

	if (!board) {
		return (
			<>
				<p className="text-body-secondary">
					Props aren't available for this game — it may have already been
					played, or the sportsbook may not support props for this sport yet.
				</p>
				<a href={helpers.leagueUrl(["sportsbook"])}>Back to Sportsbook</a>
			</>
		);
	}

	const selectedKeys = slip.selectedKeys;
	const sub = `${board.away.abbrev} @ ${board.home.abbrev}`;

	const playerTable = (p: PlayerRow) => (
		<div key={p.pid} className="col-lg-6 mb-3">
			<div className="card h-100">
				<div className="card-header py-2">
					<a
						href={helpers.leagueUrl(["player", p.pid])}
						className="fw-bold text-body"
					>
						{p.name}
					</a>{" "}
					<span className="text-body-secondary small">{p.abbrev}</span>
				</div>
				<div className="card-body py-2">
					<table className={TABLE_CLASS} style={{ marginBottom: 0 }}>
						<thead>
							<tr>
								<th>Prop</th>
								<th className="text-center" style={{ width: 84 }}>
									Over
								</th>
								<th className="text-center" style={{ width: 84 }}>
									Under
								</th>
							</tr>
						</thead>
						<tbody>
							{STAT_ORDER.map((stat) => {
								const row = p.props.find((r) => r.stat === stat);
								if (!row) {
									return null;
								}
								const overKey = `pp-${gid}-${p.pid}-${stat}-over`;
								const underKey = `pp-${gid}-${p.pid}-${stat}-under`;
								const label = STAT_LABELS[stat] ?? stat;
								return (
									<tr key={stat}>
										<td>
											{label}{" "}
											<span className="text-body-secondary small">
												{row.line}
											</span>
										</td>
										<td>
											<OddsCell
												odds={row.over}
												selected={selectedKeys.has(overKey)}
												onClick={() =>
													slip.togglePick({
														key: overKey,
														market: {
															type: "playerProp",
															gid,
															pid: p.pid,
															stat: stat as any,
															side: "over",
															line: row.line,
														},
														odds: row.over,
														title: `${p.name} Over ${row.line} ${label}`,
														sub,
													})
												}
											/>
										</td>
										<td>
											<OddsCell
												odds={row.under}
												selected={selectedKeys.has(underKey)}
												onClick={() =>
													slip.togglePick({
														key: underKey,
														market: {
															type: "playerProp",
															gid,
															pid: p.pid,
															stat: stat as any,
															side: "under",
															line: row.line,
														},
														odds: row.under,
														title: `${p.name} Under ${row.line} ${label}`,
														sub,
													})
												}
											/>
										</td>
									</tr>
								);
							})}
							{(["dd", "td"] as const).map((milestone) => {
								const key = `pm-${gid}-${p.pid}-${milestone}`;
								const odds =
									milestone === "dd" ? p.doubleDouble : p.tripleDouble;
								return (
									<tr key={milestone}>
										<td>
											{milestone === "dd" ? "Double-Double" : "Triple-Double"}
										</td>
										<td colSpan={2}>
											<OddsCell
												odds={odds}
												selected={selectedKeys.has(key)}
												onClick={() =>
													slip.togglePick({
														key,
														market: {
															type: "playerMilestone",
															gid,
															pid: p.pid,
															milestone,
														},
														odds,
														title: `${p.name} ${milestone === "dd" ? "Double-Double" : "Triple-Double"}`,
														sub,
													})
												}
											/>
										</td>
									</tr>
								);
							})}
						</tbody>
					</table>
				</div>
			</div>
		</div>
	);

	const teamPropsTable = (
		team: typeof board.home,
		heading: string,
	) => (
		<div className="col-md-6 mb-3">
			<h3 className="h5">{heading} Team Props</h3>
			{team.teamProps.length === 0 ? (
				<p className="text-body-secondary small">No team props.</p>
			) : (
				<table className={TABLE_CLASS} style={{ maxWidth: 420 }}>
					<thead>
						<tr>
							<th>Prop</th>
							<th className="text-center" style={{ width: 84 }}>
								Over
							</th>
							<th className="text-center" style={{ width: 84 }}>
								Under
							</th>
						</tr>
					</thead>
					<tbody>
						{team.teamProps.map((row) => {
							const overKey = `tp-${gid}-${team.tid}-${row.stat}-over`;
							const underKey = `tp-${gid}-${team.tid}-${row.stat}-under`;
							const label = STAT_LABELS[row.stat] ?? row.stat;
							return (
								<tr key={row.stat}>
									<td>
										{label}{" "}
										<span className="text-body-secondary small">
											{row.line}
										</span>
									</td>
									<td>
										<OddsCell
											odds={row.over}
											selected={selectedKeys.has(overKey)}
											onClick={() =>
												slip.togglePick({
													key: overKey,
													market: {
														type: "teamGameProp",
														gid,
														tid: team.tid,
														stat: row.stat as any,
														side: "over",
														line: row.line,
													},
													odds: row.over,
													title: `${team.abbrev} Over ${row.line} Team ${label}`,
													sub,
												})
											}
										/>
									</td>
									<td>
										<OddsCell
											odds={row.under}
											selected={selectedKeys.has(underKey)}
											onClick={() =>
												slip.togglePick({
													key: underKey,
													market: {
														type: "teamGameProp",
														gid,
														tid: team.tid,
														stat: row.stat as any,
														side: "under",
														line: row.line,
													},
													odds: row.under,
													title: `${team.abbrev} Under ${row.line} Team ${label}`,
													sub,
												})
											}
										/>
									</td>
								</tr>
							);
						})}
					</tbody>
				</table>
			)}
		</div>
	);

	return (
		<>
			<div className="d-flex align-items-center gap-2 mb-3">
				<Logo tid={board.away.tid} size={28} />
				<span className="fw-bold">
					{board.away.region} {board.away.name}
				</span>
				<span className="text-body-secondary">@</span>
				<Logo tid={board.home.tid} size={28} />
				<span className="fw-bold">
					{board.home.region} {board.home.name}
				</span>
			</div>
			<p>
				<a href={helpers.leagueUrl(["sportsbook"])}>
					← Back to full board
				</a>
			</p>

			<div className="row">
				<div className="col-lg-8 col-xl-9">
					<div className="row">
						{teamPropsTable(board.home, board.home.abbrev)}
						{teamPropsTable(board.away, board.away.abbrev)}
					</div>

					{board.overtime !== undefined ? (
						<div className="mb-3">
							<h3 className="h5">Game Props</h3>
							<div style={{ maxWidth: 200 }}>
								<div className="d-flex justify-content-between align-items-center">
									<span>Overtime?</span>
									<OddsCell
										odds={board.overtime}
										selected={selectedKeys.has(`gp-${gid}-overtime`)}
										onClick={() =>
											slip.togglePick({
												key: `gp-${gid}-overtime`,
												market: { type: "gameProp", gid, prop: "overtime" },
												odds: board.overtime!,
												title: "Overtime",
												sub,
											})
										}
									/>
								</div>
							</div>
						</div>
					) : null}

					<h2 className="h4">{board.home.abbrev} Player Props</h2>
					{board.home.players.length === 0 ? (
						<p className="text-body-secondary">
							No players with enough of a track record to price yet.
						</p>
					) : (
						<div className="row">{board.home.players.map(playerTable)}</div>
					)}

					<h2 className="h4 mt-3">{board.away.abbrev} Player Props</h2>
					{board.away.players.length === 0 ? (
						<p className="text-body-secondary">
							No players with enough of a track record to price yet.
						</p>
					) : (
						<div className="row">{board.away.players.map(playerTable)}</div>
					)}
				</div>
				<div className="col-lg-4 col-xl-3">
					<div className="position-sticky" style={{ top: "1rem" }}>
						<BetSlipCard slip={slip} balance={wallet.balance} />
					</div>
				</div>
			</div>

			<p className="text-body-secondary small mt-2">
				Season {season} · Play money — completely separate from the real
				game.
			</p>
		</>
	);
};

export default SportsbookGame;
