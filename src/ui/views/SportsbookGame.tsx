import { useState } from "react";
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
import {
	formatSportsbookMoney,
	formatSportsbookMoneyFull,
} from "../../common/sportsbook.ts";

// A single game's full betting board: main lines up top, then tabbed prop
// markets (one tab per stat, every player from both teams in one table) so
// nothing requires endless scrolling.

const TABLE_CLASS =
	"table table-striped table-borderless table-sm align-middle mb-0";

type GameBoard = NonNullable<View<"sportsbookGame">["board"]>;
type PlayerRow = GameBoard["home"]["players"][number];

// One tab per player-prop market.
const STAT_TABS: { key: string; label: string; long: string }[] = [
	{ key: "pts", label: "PTS", long: "Points" },
	{ key: "trb", label: "REB", long: "Rebounds" },
	{ key: "ast", label: "AST", long: "Assists" },
	{ key: "stl", label: "STL", long: "Steals" },
	{ key: "blk", label: "BLK", long: "Blocks" },
	{ key: "tp", label: "3PM", long: "3-Pointers Made" },
	{ key: "tov", label: "TO", long: "Turnovers" },
	{ key: "pra", label: "P+R+A", long: "Points + Rebounds + Assists" },
	{ key: "pr", label: "P+R", long: "Points + Rebounds" },
	{ key: "pa", label: "P+A", long: "Points + Assists" },
	{ key: "milestones", label: "DD/TD", long: "Double-Double / Triple-Double" },
];

const TEAM_PROP_LABELS: Record<string, string> = {
	pts: "Points",
	trb: "Rebounds",
	ast: "Assists",
	tp: "3-Pointers Made",
};

const signed = (n: number) => (n > 0 ? `+${n}` : `${n}`);

const SportsbookGame = ({ gid, board, wallet, season }: View<"sportsbookGame">) => {
	useTitleBar({ title: "Game Props" });

	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const slip = useBetSlip(wallet.tid);
	const [tab, setTab] = useState<string>("game");
	const [slipOpenMobile, setSlipOpenMobile] = useState(false);

	const Logo = ({ tid, size = 24 }: { tid: number; size?: number }) => {
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
					No lines for this game — it may have already been played.
				</p>
				<a href={helpers.leagueUrl(["sportsbook"])}>Back to Sportsbook</a>
			</>
		);
	}

	const selectedKeys = slip.selectedKeys;
	const sub = `${board.away.abbrev} @ ${board.home.abbrev}`;
	const allPlayers: PlayerRow[] = [...board.away.players, ...board.home.players];

	const togglePick: typeof slip.togglePick = (pick) => {
		slip.togglePick(pick);
		setSlipOpenMobile(true);
	};

	// ---- Main lines card (always visible) ---------------------------------
	const mainLines = board.main ? (
		<div className="card mb-3">
			<div className="card-body p-2">
				<div className="table-responsive">
					<table className={TABLE_CLASS} style={{ maxWidth: 560 }}>
						<thead>
							<tr className="text-body-secondary small">
								<th />
								<th className="text-center" style={{ width: 90 }}>
									Spread
								</th>
								<th className="text-center" style={{ width: 90 }}>
									Total
								</th>
								<th className="text-center" style={{ width: 90 }}>
									Money
								</th>
							</tr>
						</thead>
						<tbody>
							{(
								[
									{
										side: board.away,
										spreadLine: -board.main.spread.line,
										spreadOdds: board.main.spread.away,
										totKind: "over" as const,
										totOdds: board.main.total.over,
										ml: board.main.moneyline.away,
									},
									{
										side: board.home,
										spreadLine: board.main.spread.line,
										spreadOdds: board.main.spread.home,
										totKind: "under" as const,
										totOdds: board.main.total.under,
										ml: board.main.moneyline.home,
									},
								]
							).map((r) => (
								<tr key={r.side.tid}>
									<td>
										<div className="d-flex align-items-center gap-2">
											<Logo tid={r.side.tid} size={20} />
											<span className="fw-medium">
												{r.side.region} {r.side.name}
											</span>
										</div>
									</td>
									<td>
										<OddsCell
											line={signed(r.spreadLine)}
											odds={r.spreadOdds}
											selected={selectedKeys.has(`sp-${gid}-${r.side.tid}`)}
											onClick={() =>
												togglePick({
													key: `sp-${gid}-${r.side.tid}`,
													market: {
														type: "gameSpread",
														gid,
														pickTid: r.side.tid,
														line: r.spreadLine,
													},
													odds: r.spreadOdds,
													title: `${r.side.abbrev} ${signed(r.spreadLine)}`,
													sub,
												})
											}
										/>
									</td>
									<td>
										<OddsCell
											line={`${r.totKind === "over" ? "O" : "U"} ${board.main!.total.line}`}
											odds={r.totOdds}
											selected={selectedKeys.has(`tot-${gid}-${r.totKind}`)}
											onClick={() =>
												togglePick({
													key: `tot-${gid}-${r.totKind}`,
													market: {
														type: "gameTotal",
														gid,
														side: r.totKind,
														line: board.main!.total.line,
													},
													odds: r.totOdds,
													title: `${r.totKind === "over" ? "Over" : "Under"} ${board.main!.total.line}`,
													sub,
												})
											}
										/>
									</td>
									<td>
										<OddsCell
											odds={r.ml}
											selected={selectedKeys.has(`ml-${gid}-${r.side.tid}`)}
											onClick={() =>
												togglePick({
													key: `ml-${gid}-${r.side.tid}`,
													market: {
														type: "gameMoneyline",
														gid,
														pickTid: r.side.tid,
													},
													odds: r.ml,
													title: `${r.side.abbrev} ML`,
													sub,
												})
											}
										/>
									</td>
								</tr>
							))}
						</tbody>
					</table>
				</div>
			</div>
		</div>
	) : null;

	// ---- Game tab: team props + OT ----------------------------------------
	const teamPropsCard = (team: GameBoard["home"]) => (
		<div className="card mb-3">
			<div className="card-header py-2 d-flex align-items-center gap-2">
				<Logo tid={team.tid} size={20} />
				<span className="fw-bold">{team.abbrev} Team Props</span>
			</div>
			<div className="card-body p-2">
				<table className={TABLE_CLASS} style={{ maxWidth: 420 }}>
					<thead>
						<tr className="text-body-secondary small">
							<th>Market</th>
							<th className="text-center" style={{ width: 90 }}>
								Over
							</th>
							<th className="text-center" style={{ width: 90 }}>
								Under
							</th>
						</tr>
					</thead>
					<tbody>
						{team.teamProps.map((row) => {
							const overKey = `tp-${gid}-${team.tid}-${row.stat}-over`;
							const underKey = `tp-${gid}-${team.tid}-${row.stat}-under`;
							const label = TEAM_PROP_LABELS[row.stat] ?? row.stat;
							return (
								<tr key={row.stat}>
									<td>
										{label}{" "}
										<span className="text-body-secondary">{row.line}</span>
									</td>
									{(["over", "under"] as const).map((side) => (
										<td key={side}>
											<OddsCell
												odds={side === "over" ? row.over : row.under}
												selected={selectedKeys.has(
													side === "over" ? overKey : underKey,
												)}
												onClick={() =>
													togglePick({
														key: side === "over" ? overKey : underKey,
														market: {
															type: "teamGameProp",
															gid,
															tid: team.tid,
															stat: row.stat as any,
															side,
															line: row.line,
														},
														odds: side === "over" ? row.over : row.under,
														title: `${team.abbrev} ${side === "over" ? "Over" : "Under"} ${row.line} Team ${label}`,
														sub,
													})
												}
											/>
										</td>
									))}
								</tr>
							);
						})}
					</tbody>
				</table>
			</div>
		</div>
	);

	const gameTab = (
		<>
			<div className="row">
				<div className="col-md-6">{teamPropsCard(board.away)}</div>
				<div className="col-md-6">{teamPropsCard(board.home)}</div>
			</div>
			{board.overtime !== undefined ? (
				<div className="card" style={{ maxWidth: 260 }}>
					<div className="card-body p-2 d-flex align-items-center justify-content-between">
						<span className="fw-medium">Goes to overtime</span>
						<div style={{ width: 90 }}>
							<OddsCell
								odds={board.overtime}
								selected={selectedKeys.has(`gp-${gid}-overtime`)}
								onClick={() =>
									togglePick({
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
		</>
	);

	// ---- Player prop tabs ---------------------------------------------------
	const statTab = (statKey: string) => {
		const rows = allPlayers
			.map((p) => ({
				p,
				prop: p.props.find((r) => r.stat === statKey),
			}))
			.filter((x) => x.prop !== undefined)
			.sort((a, b) => b.prop!.line - a.prop!.line);

		if (rows.length === 0) {
			return (
				<p className="text-body-secondary">
					No lines here yet — players need a track record first.
				</p>
			);
		}

		return (
			<div className="card">
				<div className="card-body p-2">
					<div className="table-responsive">
						<table className={TABLE_CLASS} style={{ maxWidth: 560 }}>
							<thead>
								<tr className="text-body-secondary small">
									<th>Player</th>
									<th className="text-end" style={{ width: 60 }}>
										Line
									</th>
									<th className="text-center" style={{ width: 90 }}>
										Over
									</th>
									<th className="text-center" style={{ width: 90 }}>
										Under
									</th>
								</tr>
							</thead>
							<tbody>
								{rows.map(({ p, prop }) => {
									const overKey = `pp-${gid}-${p.pid}-${statKey}-over`;
									const underKey = `pp-${gid}-${p.pid}-${statKey}-under`;
									const long =
										STAT_TABS.find((t) => t.key === statKey)?.long ?? statKey;
									return (
										<tr key={p.pid}>
											<td>
												<a href={helpers.leagueUrl(["player", p.pid])}>
													{p.name}
												</a>{" "}
												<span className="text-body-secondary small">
													{p.abbrev}
												</span>
											</td>
											<td className="text-end fw-medium">{prop!.line}</td>
											{(["over", "under"] as const).map((side) => (
												<td key={side}>
													<OddsCell
														odds={side === "over" ? prop!.over : prop!.under}
														selected={selectedKeys.has(
															side === "over" ? overKey : underKey,
														)}
														onClick={() =>
															togglePick({
																key: side === "over" ? overKey : underKey,
																market: {
																	type: "playerProp",
																	gid,
																	pid: p.pid,
																	stat: statKey as any,
																	side,
																	line: prop!.line,
																},
																odds:
																	side === "over" ? prop!.over : prop!.under,
																title: `${p.name} ${side === "over" ? "Over" : "Under"} ${prop!.line} ${long}`,
																sub,
															})
														}
													/>
												</td>
											))}
										</tr>
									);
								})}
							</tbody>
						</table>
					</div>
				</div>
			</div>
		);
	};

	const milestonesTab = (
		<div className="card">
			<div className="card-body p-2">
				<div className="table-responsive">
					<table className={TABLE_CLASS} style={{ maxWidth: 480 }}>
						<thead>
							<tr className="text-body-secondary small">
								<th>Player</th>
								<th className="text-center" style={{ width: 100 }}>
									Double-Double
								</th>
								<th className="text-center" style={{ width: 100 }}>
									Triple-Double
								</th>
							</tr>
						</thead>
						<tbody>
							{allPlayers.map((p) => {
								const ddKey = `pm-${gid}-${p.pid}-dd`;
								const tdKey = `pm-${gid}-${p.pid}-td`;
								return (
									<tr key={p.pid}>
										<td>
											<a href={helpers.leagueUrl(["player", p.pid])}>
												{p.name}
											</a>{" "}
											<span className="text-body-secondary small">
												{p.abbrev}
											</span>
										</td>
										{(["dd", "td"] as const).map((milestone) => (
											<td key={milestone}>
												<OddsCell
													odds={
														milestone === "dd"
															? p.doubleDouble
															: p.tripleDouble
													}
													selected={selectedKeys.has(
														milestone === "dd" ? ddKey : tdKey,
													)}
													onClick={() =>
														togglePick({
															key: milestone === "dd" ? ddKey : tdKey,
															market: {
																type: "playerMilestone",
																gid,
																pid: p.pid,
																milestone,
															},
															odds:
																milestone === "dd"
																	? p.doubleDouble
																	: p.tripleDouble,
															title: `${p.name} ${milestone === "dd" ? "Double-Double" : "Triple-Double"}`,
															sub,
														})
													}
												/>
											</td>
										))}
									</tr>
								);
							})}
						</tbody>
					</table>
				</div>
			</div>
		</div>
	);

	const betslip = <BetSlipCard slip={slip} balance={wallet.balance} />;

	return (
		<>
			<div className="d-flex flex-wrap align-items-center gap-2 mb-1">
				<Logo tid={board.away.tid} size={32} />
				<span className="h5 mb-0 fw-bold">
					{board.away.region} {board.away.name}
				</span>
				<span className="text-body-secondary">@</span>
				<Logo tid={board.home.tid} size={32} />
				<span className="h5 mb-0 fw-bold">
					{board.home.region} {board.home.name}
				</span>
				<span className="badge text-bg-secondary ms-auto" title="Your balance">
					{formatSportsbookMoneyFull(wallet.balance)}
				</span>
			</div>
			<p className="mb-3">
				<a href={helpers.leagueUrl(["sportsbook"])}>← All games</a>
			</p>

			<div className="row">
				<div className="col-lg-8 col-xl-9">
					{mainLines}

					<ul className="nav nav-tabs mb-3 flex-nowrap overflow-auto text-nowrap">
						<li className="nav-item">
							<button
								className={`nav-link ${tab === "game" ? "active" : ""}`}
								onClick={() => setTab("game")}
							>
								Team
							</button>
						</li>
						{STAT_TABS.map((t) => (
							<li className="nav-item" key={t.key}>
								<button
									className={`nav-link ${tab === t.key ? "active" : ""}`}
									title={t.long}
									onClick={() => setTab(t.key)}
								>
									{t.label}
								</button>
							</li>
						))}
					</ul>

					{tab === "game"
						? gameTab
						: tab === "milestones"
							? milestonesTab
							: statTab(tab)}
				</div>
				<div className="col-lg-4 col-xl-3 d-none d-lg-block">
					<div className="position-sticky" style={{ top: "1rem" }}>
						{betslip}
					</div>
				</div>
			</div>

			{slip.picks.length > 0 ? (
				<div
					className="d-lg-none position-fixed start-0 end-0 bottom-0 p-2"
					style={{ zIndex: 1030 }}
				>
					{slipOpenMobile ? (
						<div className="mx-auto" style={{ maxWidth: 520 }}>
							{betslip}
							<button
								className="btn btn-sm btn-secondary w-100 mt-1"
								onClick={() => setSlipOpenMobile(false)}
							>
								Hide slip
							</button>
						</div>
					) : (
						<button
							className="btn btn-primary w-100 shadow"
							onClick={() => setSlipOpenMobile(true)}
						>
							Bet Slip ({slip.picks.length}) ·{" "}
							{slip.totalStake > 0
								? `${formatSportsbookMoney(slip.totalStake)} to win ${formatSportsbookMoney(slip.totalPayout - slip.totalStake)}`
								: "add stake"}
						</button>
					)}
				</div>
			) : null}

			<p className="text-body-secondary small mt-3">Season {season}</p>
		</>
	);
};

export default SportsbookGame;
