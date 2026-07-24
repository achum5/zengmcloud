import { Fragment, useEffect, useState, type ReactNode } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { toWorker } from "../util/toWorker.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { showNotification } from "../util/showNotification.ts";
import type { View } from "../../common/types.ts";
import type { SportsbookMarket } from "../../common/types.ts";
import { PHASE } from "../../common/constants.ts";
import { useLocal } from "../util/local.ts";
import { TeamLogoInline } from "../components/TeamLogoInline.tsx";
import {
	BetSlipCard,
	OddsCell,
	useBetSlip,
	type Pick,
} from "../components/SportsbookBetSlip.tsx";
import {
	formatAmerican,
	formatSportsbookMoney,
	formatSportsbookMoneyFull,
	marketGid,
} from "../../common/sportsbook.ts";

// Surfaces: a raised panel for a bet/game, and a slightly stronger tone for the
// header strip on top of it. Everything on this page is built from these two so
// the board, the slips, and the futures cards read as one system.
const PANEL = "var(--bs-tertiary-bg)";
const PANEL_HEADER = "var(--bs-secondary-bg)";

// How each bet outcome is presented: the badge, and the color of the stripe
// down the left edge of its card (the thing that makes a long settled list
// scannable at a glance instead of a wall of identical rows).
const RESULT_META = {
	won: { label: "Won", badge: "text-bg-success", stripe: "var(--bs-success)" },
	lost: { label: "Lost", badge: "text-bg-danger", stripe: "var(--bs-danger)" },
	push: {
		label: "Push",
		badge: "text-bg-secondary",
		stripe: "var(--bs-secondary)",
	},
	void: {
		label: "Void",
		badge: "text-bg-secondary",
		stripe: "var(--bs-secondary)",
	},
	open: {
		label: "Open",
		badge: "text-bg-warning",
		stripe: "var(--bs-warning)",
	},
} as const;

// A bet's stored label is built as "<title> — <sub>". Bets placed before those
// two halves were de-duplicated repeat the market name in both ("Tyson Chandler
// — Makes the All-Rookie Team — Makes the All-Rookie Team", "SA — Champion —
// Championship"), so collapse a segment that repeats - or is just a prefix of -
// the one beside it. Display-only; the stored label is left alone.
const cleanLabel = (label: string): string => {
	const out: string[] = [];
	for (const part of label.split(" — ")) {
		const prev = out.at(-1);
		if (prev !== undefined && prev.length >= 5) {
			if (prev === part || part.startsWith(prev)) {
				out[out.length - 1] = part;
				continue;
			}
			if (prev.startsWith(part)) {
				continue;
			}
		}
		out.push(part);
	}
	return out.join(" — ");
};

// A team's logo by tid. Module level (rather than defined inside the page) so
// it isn't re-created on every render, which would reset the image's state.
const Logo = ({ tid, size = 20 }: { tid: number; size?: number }) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
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

const Sportsbook = ({
	board,
	wallet,
	balances,
	leagueBets,
	gameLinks,
	season,
	phase,
}: View<"sportsbook">) => {
	useTitleBar({ title: "Sportsbook" });

	const { teamInfoCache } = useLocal(["teamInfoCache"]);

	const [tab, setTab] = useState<
		"games" | "futures" | "awards" | "mybets" | "leaguebets"
	>("games");
	const [slipOpenMobile, setSlipOpenMobile] = useState(false);
	const slip = useBetSlip(wallet.tid, {
		allStarRosterSize: board.allStarRosterSize,
	});

	// Catch up any bet whose outcome is already known but that a missed hook
	// didn't settle (e.g. this device was offline when a phase change decided
	// an award). A real captured worker call - see worker/api/index.ts
	// sportsbookSettle - so a payout it produces actually publishes to a
	// synced room instead of only applying to this device's local cache.
	useEffect(() => {
		void toWorker("main", "sportsbookSettle", undefined);
	}, []);

	const teamName = (tid: number) => {
		const t = teamInfoCache[tid];
		return t ? `${t.region} ${t.name}` : "?";
	};
	const teamAbbrev = (tid: number) => teamInfoCache[tid]?.abbrev ?? "???";
	const teamLink = (tid: number, text: ReactNode) => (
		<a
			href={helpers.leagueUrl(["roster", `${teamAbbrev(tid)}_${tid}`])}
			className="text-truncate"
		>
			{text}
		</a>
	);

	const selectedKeys = slip.selectedKeys;
	const togglePick = (pick: Pick) => {
		slip.togglePick(pick);
		setSlipOpenMobile(true);
	};

	const signed = (n: number) => (n > 0 ? `+${n}` : `${n}`);

	const cancelBet = async (betID: number) => {
		try {
			await toWorker("main", "sportsbookCancelBet", { tid: wallet.tid, betID });
			await realtimeUpdate(["watchList"]);
		} catch (error) {
			showNotification({
				type: "error",
				text: error instanceof Error ? error.message : "Could not cancel.",
			});
		}
	};

	const betslip = <BetSlipCard slip={slip} balance={wallet.balance} />;

	const emptyState = (text: string) => (
		<div
			className="rounded-3 p-4 text-center text-body-secondary"
			style={{ background: PANEL }}
		>
			{text}
		</div>
	);

	// A titled panel used by the futures and awards boards.
	const boardCard = (title: ReactNode, body: ReactNode, key?: string) => (
		<div
			key={key}
			className="rounded-3 overflow-hidden h-100"
			style={{ background: PANEL }}
		>
			<div
				className="px-3 py-2 fw-bold small text-uppercase"
				style={{ background: PANEL_HEADER, letterSpacing: "0.03em" }}
			>
				{title}
			</div>
			<div className="p-2">{body}</div>
		</div>
	);

	// One selectable line on a futures/awards board: label on the left, the odds
	// button pinned right. A flex row (not a table cell) so a long team or player
	// name truncates instead of pushing the price off screen.
	const marketRow = (
		key: string,
		label: ReactNode,
		odds: number,
		pick: () => Pick,
	) => (
		<div
			key={key}
			className="d-flex align-items-center gap-2 px-1 py-1"
			style={{ minWidth: 0 }}
		>
			<div className="flex-grow-1 text-truncate" style={{ minWidth: 0 }}>
				{label}
			</div>
			<div className="flex-shrink-0" style={{ width: 78 }}>
				<OddsCell
					odds={odds}
					selected={selectedKeys.has(key)}
					onClick={() => togglePick(pick())}
				/>
			</div>
		</div>
	);

	// ---- Games -----------------------------------------------------------
	// Each game is its own panel with a three-column odds grid, so a full board
	// reads top-to-bottom on a phone instead of scrolling sideways off a table.
	const gamesTab =
		board.games.length === 0 ? (
			emptyState("No upcoming games to bet on right now.")
		) : (
			<div style={{ maxWidth: 680 }}>
				{board.games.map((game) => {
					const sub = `${teamAbbrev(game.away.tid)} @ ${teamAbbrev(game.home.tid)}`;
					const rows = [
						{
							t: game.away,
							spreadLine: -game.spread.line,
							spreadOdds: game.spread.away,
							ml: game.moneyline.away,
							totKind: "over" as const,
							totOdds: game.total.over,
						},
						{
							t: game.home,
							spreadLine: game.spread.line,
							spreadOdds: game.spread.home,
							ml: game.moneyline.home,
							totKind: "under" as const,
							totOdds: game.total.under,
						},
					];

					return (
						<div
							key={game.gid}
							className="rounded-3 overflow-hidden mb-2"
							style={{ background: PANEL }}
						>
							<div
								className="d-flex align-items-center gap-2 px-3 py-1"
								style={{ background: PANEL_HEADER }}
							>
								<span className="small text-body-secondary text-truncate">
									{sub}
								</span>
								<a
									className="ms-auto small text-nowrap"
									href={helpers.leagueUrl(["sportsbook", "game", game.gid])}
								>
									Props →
								</a>
							</div>
							<div className="p-2">
								<div
									style={{
										display: "grid",
										// Fixed odds columns, so a narrow screen shrinks the team
										// name (which truncates) and never the buttons.
										gridTemplateColumns: "minmax(0, 1fr) repeat(3, 74px)",
										gap: "4px",
										alignItems: "center",
									}}
								>
									<div />
									{["Spread", "Money", "Total"].map((label) => (
										<div
											key={label}
											className="text-center text-body-secondary text-uppercase"
											style={{ fontSize: "0.68rem", letterSpacing: "0.03em" }}
										>
											{label}
										</div>
									))}

									{rows.map((r) => {
										const spreadKey = `sp-${game.gid}-${r.t.tid}`;
										const mlKey = `ml-${game.gid}-${r.t.tid}`;
										const totKey = `tot-${game.gid}-${r.totKind}`;
										return (
											<Fragment key={r.t.tid}>
												<div
													className="d-flex align-items-center gap-2"
													style={{ minWidth: 0 }}
												>
													<Logo tid={r.t.tid} />
													<div className="lh-sm" style={{ minWidth: 0 }}>
														<div className="text-truncate">
															{teamLink(
																r.t.tid,
																<>
																	{/* Full name where there's room; just the
																	    nickname on a phone, where the header
																	    strip already names the matchup - a
																	    truncated "Denver Nug…" reads worse. */}
																	<span className="d-none d-sm-inline">
																		{r.t.region} {r.t.name}
																	</span>
																	<span className="d-sm-none">{r.t.name}</span>
																</>,
															)}
														</div>
														<div
															className="text-body-secondary text-truncate"
															style={{ fontSize: "0.75rem" }}
														>
															{r.t.won}-{r.t.lost}
															{r.t.ats ? ` · ${r.t.ats} ATS` : ""}
														</div>
													</div>
												</div>
												<OddsCell
													line={signed(r.spreadLine)}
													odds={r.spreadOdds}
													selected={selectedKeys.has(spreadKey)}
													onClick={() =>
														togglePick({
															key: spreadKey,
															market: {
																type: "gameSpread",
																gid: game.gid,
																pickTid: r.t.tid,
																line: r.spreadLine,
															},
															odds: r.spreadOdds,
															title: `${teamAbbrev(r.t.tid)} ${signed(r.spreadLine)}`,
															sub,
														})
													}
												/>
												<OddsCell
													odds={r.ml}
													selected={selectedKeys.has(mlKey)}
													onClick={() =>
														togglePick({
															key: mlKey,
															market: {
																type: "gameMoneyline",
																gid: game.gid,
																pickTid: r.t.tid,
															},
															odds: r.ml,
															title: `${teamAbbrev(r.t.tid)} ML`,
															sub,
														})
													}
												/>
												<OddsCell
													line={`${r.totKind === "over" ? "O" : "U"} ${game.total.line}`}
													odds={r.totOdds}
													selected={selectedKeys.has(totKey)}
													onClick={() =>
														togglePick({
															key: totKey,
															market: {
																type: "gameTotal",
																gid: game.gid,
																side: r.totKind,
																line: game.total.line,
															},
															odds: r.totOdds,
															title: `${r.totKind === "over" ? "Over" : "Under"} ${game.total.line}`,
															sub,
														})
													}
												/>
											</Fragment>
										);
									})}
								</div>
							</div>
						</div>
					);
				})}
			</div>
		);

	// ---- Futures ---------------------------------------------------------
	const teamFuturesCard = (
		heading: string,
		rows: {
			tid: number;
			abbrev: string;
			region: string;
			name: string;
			americanOdds: number;
		}[],
		mk: (tid: number) => SportsbookMarket,
		keyPrefix: string,
	) =>
		boardCard(
			heading,
			<div style={{ maxHeight: 360, overflowY: "auto" }}>
				{rows.map((r) =>
					marketRow(
						`${keyPrefix}-${r.tid}`,
						<div
							className="d-flex align-items-center gap-2"
							style={{ minWidth: 0 }}
						>
							<Logo tid={r.tid} />
							{teamLink(r.tid, `${r.region} ${r.name}`)}
						</div>,
						r.americanOdds,
						() => ({
							key: `${keyPrefix}-${r.tid}`,
							market: mk(r.tid),
							// The market name lives in `sub`; keeping it out of `title`
							// stops the stored label repeating it twice.
							title: teamAbbrev(r.tid),
							odds: r.americanOdds,
							sub: heading,
						}),
					),
				)}
			</div>,
			keyPrefix,
		);

	const futuresTab = (
		<div className="row g-3">
			<div className="col-xl-6">
				<div className="mb-3">
					{teamFuturesCard(
						"Championship",
						board.championship,
						(tid) => ({ type: "champion", pickTid: tid, season }),
						"champ",
					)}
				</div>
				{board.conferences.map((conf) => (
					<div className="mb-3" key={conf.cid}>
						{teamFuturesCard(
							`${conf.name} Winner`,
							conf.teams,
							(tid) => ({
								type: "conf",
								pickTid: tid,
								cid: conf.cid,
								season,
							}),
							`conf${conf.cid}`,
						)}
					</div>
				))}
			</div>
			<div className="col-xl-6">
				{board.winTotals.length > 0 ? (
					<div className="mb-3">
						{boardCard(
							"Season Win Totals",
							<div style={{ maxHeight: 420, overflowY: "auto" }}>
								{board.winTotals.map((t) => (
									<div
										key={t.tid}
										className="d-flex align-items-center gap-2 px-1 py-1"
									>
										<div
											className="d-flex align-items-center gap-2 flex-grow-1 text-truncate"
											style={{ minWidth: 0 }}
										>
											<Logo tid={t.tid} />
											{teamLink(t.tid, `${t.region} ${t.name}`)}
										</div>
										<div className="text-body-secondary flex-shrink-0 me-1">
											{t.line}
										</div>
										{(["over", "under"] as const).map((side) => {
											const key =
												side === "over" ? `wto-${t.tid}` : `wtu-${t.tid}`;
											const odds = side === "over" ? t.over : t.under;
											return (
												<div
													key={side}
													className="flex-shrink-0"
													style={{ width: 74 }}
												>
													<OddsCell
														line={side === "over" ? "O" : "U"}
														odds={odds}
														selected={selectedKeys.has(key)}
														onClick={() =>
															togglePick({
																key,
																market: {
																	type: "winTotal",
																	pickTid: t.tid,
																	side,
																	line: t.line,
																	season,
																},
																odds,
																title: `${teamAbbrev(t.tid)} ${side === "over" ? "Over" : "Under"} ${t.line}`,
																sub: "Season wins",
															})
														}
													/>
												</div>
											);
										})}
									</div>
								))}
							</div>,
						)}
					</div>
				) : null}
				{board.divisions.map((div) => (
					<div className="mb-3" key={div.did}>
						{teamFuturesCard(
							`${div.name} Winner`,
							div.teams,
							(tid) => ({ type: "div", pickTid: tid, did: div.did, season }),
							`div${div.did}`,
						)}
					</div>
				))}
			</div>
		</div>
	);

	// ---- Awards ------------------------------------------------------------
	// A player-vs-a-role market ("will X win the trophy" / "will X make the
	// team"), as opposed to the team futures above. Shared shape for all of
	// them: single-winner races (MVP, etc.), the binary All-Star/All-Rookie
	// "makes it" props, and each tier of All-League/All-Defensive.
	const playerCandidateCard = (
		heading: string,
		candidates: {
			pid: number;
			name: string;
			tid: number;
			abbrev: string;
			americanOdds: number;
		}[],
		mk: (pid: number) => SportsbookMarket,
		keyPrefix: string,
	) => (
		<div key={keyPrefix} className="col-xl-4 col-lg-6">
			{boardCard(
				heading,
				candidates.length === 0 ? (
					<p className="text-body-secondary small mb-0 px-1 py-2">
						No candidates.
					</p>
				) : (
					<div style={{ maxHeight: 360, overflowY: "auto" }}>
						{candidates.map((c) =>
							marketRow(
								`${keyPrefix}-${c.pid}`,
								<>
									<a href={helpers.leagueUrl(["player", c.pid])}>{c.name}</a>{" "}
									<span className="text-body-secondary small">
										{teamAbbrev(c.tid)}
									</span>
								</>,
								c.americanOdds,
								() => ({
									key: `${keyPrefix}-${c.pid}`,
									market: mk(c.pid),
									// `sub` carries the market name - see teamFuturesCard.
									title: c.name,
									odds: c.americanOdds,
									sub: heading,
								}),
							),
						)}
					</div>
				),
			)}
		</div>
	);

	const hasAnyAwardMarket =
		board.awards.length > 0 ||
		board.allStar.length > 0 ||
		board.allLeague.some((t) => t.candidates.length > 0) ||
		board.allDefensive.some((t) => t.candidates.length > 0) ||
		board.allRookie.length > 0;

	const awardsSection = (title: string, cards: ReactNode) => (
		<>
			<h2 className="h6 text-uppercase text-body-secondary mt-3 mb-2">
				{title}
			</h2>
			<div className="row g-3">{cards}</div>
		</>
	);

	const awardsTab = !hasAnyAwardMarket ? (
		emptyState(
			phase >= PHASE.PLAYOFFS
				? "Award futures are closed for the season — they lock when the regular season ends."
				: "No award futures available right now — check back once the season's underway.",
		)
	) : (
		<>
			{board.awards.length > 0
				? awardsSection(
						"Player Awards",
						board.awards.map((race) =>
							playerCandidateCard(
								race.name,
								race.candidates,
								(pid) => ({
									type: "award",
									award: race.award,
									pid,
									season,
								}),
								`aw-${race.award}`,
							),
						),
					)
				: null}

			{board.allStar.length > 0
				? awardsSection(
						"All-Star Team",
						playerCandidateCard(
							"Makes the All-Star Team",
							board.allStar,
							(pid) => ({ type: "allStarTeam", pid, season }),
							"as",
						),
					)
				: null}

			{board.allLeague.some((t) => t.candidates.length > 0)
				? awardsSection(
						"All-League Team",
						board.allLeague.map((t) =>
							playerCandidateCard(
								t.title,
								t.candidates,
								(pid) => ({
									type: "allLeagueTeam",
									pid,
									tier: t.tier as 1 | 2 | 3,
									season,
								}),
								`al-${t.tier}`,
							),
						),
					)
				: null}

			{board.allDefensive.some((t) => t.candidates.length > 0)
				? awardsSection(
						"All-Defensive Team",
						board.allDefensive.map((t) =>
							playerCandidateCard(
								t.title,
								t.candidates,
								(pid) => ({
									type: "allDefensiveTeam",
									pid,
									tier: t.tier as 1 | 2 | 3,
									season,
								}),
								`ad-${t.tier}`,
							),
						),
					)
				: null}

			{board.allRookie.length > 0
				? awardsSection(
						"All-Rookie Team",
						playerCandidateCard(
							"Makes the All-Rookie Team",
							board.allRookie,
							(pid) => ({ type: "allRookieTeam", pid, season }),
							"ar",
						),
					)
				: null}
		</>
	);

	// ---- Bets --------------------------------------------------------------
	// The box score of the game a market is about, when it's been played (the
	// worker only sends links for played games, so open/future bets don't link).
	const boxScoreHref = (gid?: number) => {
		if (gid === undefined) {
			return undefined;
		}
		const link = gameLinks[gid];
		return link
			? helpers.leagueUrl(["game_log", link.abbrevTid, link.season, gid])
			: undefined;
	};

	// A small per-leg outcome mark (nothing while the leg is still open).
	const legMark = (r?: string) =>
		r === "won" ? (
			<span className="text-success fw-bold flex-shrink-0">✓</span>
		) : r === "lost" ? (
			<span className="text-danger fw-bold flex-shrink-0">✗</span>
		) : r === "push" || r === "void" ? (
			<span className="badge text-bg-secondary flex-shrink-0">
				{r === "push" ? "Push" : "Void"}
			</span>
		) : null;

	// One bet as a self-contained card: what was bet and how it landed, with the
	// money on its own line. Replaces the old six-column table, which ran off the
	// side of a phone and clipped the stake and the cancel button.
	const BetCard = ({
		bet,
		canCancel,
	}: {
		bet: (typeof wallet.bets)[number];
		canCancel: boolean;
	}) => {
		const result = (bet.result ?? "open") as keyof typeof RESULT_META;
		const meta = RESULT_META[result] ?? RESULT_META.open;
		const legs = bet.legs ?? [];
		const href = boxScoreHref(marketGid(bet.market));

		// The money line: what's at stake, and what it returned (or would).
		let money: ReactNode;
		if (result === "open") {
			money = (
				<>
					{formatSportsbookMoney(bet.stake)} to win{" "}
					<span className="text-body">
						{formatSportsbookMoney(bet.stake * (bet.decimalOdds - 1))}
					</span>
				</>
			);
		} else if (result === "won") {
			money = (
				<>
					{formatSportsbookMoney(bet.stake)} returned{" "}
					<span className="text-success fw-bold">
						{formatSportsbookMoney(bet.stake * bet.decimalOdds)}
					</span>
				</>
			);
		} else if (result === "lost") {
			money = (
				<span className="text-danger">
					Lost {formatSportsbookMoney(bet.stake)}
				</span>
			);
		} else {
			money = <>{formatSportsbookMoney(bet.stake)} refunded</>;
		}

		return (
			<div
				className="rounded-2 mb-2 py-2 pe-2"
				style={{
					background: PANEL,
					borderLeft: `4px solid ${meta.stripe}`,
					paddingLeft: 12,
				}}
			>
				<div className="d-flex align-items-start gap-2">
					<div className="flex-grow-1" style={{ minWidth: 0 }}>
						{legs.length > 0 ? (
							<div className="fw-bold">{cleanLabel(bet.label)}</div>
						) : href ? (
							<a className="fw-medium" href={href}>
								{cleanLabel(bet.label)}
							</a>
						) : (
							<span className="fw-medium">{cleanLabel(bet.label)}</span>
						)}
					</div>
					<div className="text-end flex-shrink-0">
						<div className="fw-bold lh-1">
							{formatAmerican(bet.americanOdds)}
						</div>
						<span
							className={`badge ${meta.badge} mt-1`}
							style={{ fontWeight: 500 }}
						>
							{meta.label}
						</span>
					</div>
				</div>

				{legs.length > 0 ? (
					<div className="mt-2 d-flex flex-column gap-1">
						{legs.map((leg, i) => {
							const legHref = boxScoreHref(marketGid(leg.market));
							return (
								<div
									key={i}
									className="d-flex align-items-baseline gap-2 small"
									style={{ minWidth: 0 }}
								>
									<span
										className="text-body-secondary text-end flex-shrink-0"
										style={{ width: 44 }}
									>
										{formatAmerican(leg.americanOdds)}
									</span>
									<span className="flex-grow-1" style={{ minWidth: 0 }}>
										{legHref ? (
											<a href={legHref}>{cleanLabel(leg.label)}</a>
										) : (
											cleanLabel(leg.label)
										)}
									</span>
									{legMark(leg.result)}
								</div>
							);
						})}
					</div>
				) : null}

				<div className="d-flex align-items-center gap-2 mt-1 small text-body-secondary">
					<span>{money}</span>
					{canCancel && result === "open" ? (
						<button
							className="btn btn-sm btn-link text-danger text-decoration-none p-0 ms-auto"
							onClick={() => cancelBet(bet.betID)}
						>
							Cancel
						</button>
					) : null}
				</div>
			</div>
		);
	};

	const betList = (
		bets: typeof wallet.bets,
		canCancel: boolean,
		empty: string,
	) =>
		bets.length === 0 ? (
			<p className="text-body-secondary">{empty}</p>
		) : (
			<div style={{ maxWidth: 680 }}>
				{bets.map((bet) => (
					<BetCard
						key={`${bet.betID}-${bet.result ?? "open"}`}
						bet={bet}
						canCancel={canCancel}
					/>
				))}
			</div>
		);

	const myBetsTab = (
		<>
			<h2 className="h6 text-uppercase text-body-secondary mb-2">
				Open Bets
				{wallet.bets.length > 0 ? ` (${wallet.bets.length})` : ""}
			</h2>
			{betList(wallet.bets, true, "No open bets.")}

			<h2 className="h6 text-uppercase text-body-secondary mt-4 mb-2">
				Settled
			</h2>
			{betList(wallet.history, false, "Nothing settled yet.")}
		</>
	);

	// Every user team's slips - league-mates can see what each other has riding.
	const leagueBetsTab = (
		<div style={{ maxWidth: 680 }}>
			{leagueBets.map((team) => (
				<div
					key={team.tid}
					className="rounded-3 overflow-hidden mb-3"
					style={{ background: PANEL }}
				>
					<div
						className="d-flex align-items-center gap-2 px-3 py-2"
						style={{ background: PANEL_HEADER }}
					>
						<Logo tid={team.tid} size={20} />
						<span className="fw-bold text-truncate">{teamName(team.tid)}</span>
						{team.tid === wallet.tid ? (
							<span className="badge text-bg-primary">You</span>
						) : null}
						<span className="ms-auto text-body-secondary text-nowrap">
							{formatSportsbookMoneyFull(team.balance)}
						</span>
					</div>
					<div className="p-2">
						{team.open.length === 0 && team.settled.length === 0 ? (
							<p className="text-body-secondary small mb-0 px-1 py-2">
								No bets yet.
							</p>
						) : (
							[...team.open, ...team.settled].map((bet) => (
								<BetCard
									key={`${bet.betID}-${bet.result ?? "open"}`}
									bet={bet}
									canCancel={false}
								/>
							))
						)}
					</div>
				</div>
			))}
		</div>
	);

	// ---- Header ------------------------------------------------------------
	// A quick read on how the book is treating you: what's still live, and how
	// the settled slips have gone.
	const atRisk = wallet.bets.reduce((sum, bet) => sum + bet.stake, 0);
	const settledWon = wallet.history.filter((b) => b.result === "won").length;
	const settledLost = wallet.history.filter((b) => b.result === "lost").length;
	const net = wallet.history.reduce((sum, bet) => {
		if (bet.result === "won") {
			return sum + bet.stake * (bet.decimalOdds - 1);
		}
		if (bet.result === "lost") {
			return sum - bet.stake;
		}
		return sum;
	}, 0);

	return (
		<>
			<div className="rounded-3 p-3 mb-3" style={{ background: PANEL }}>
				<div className="d-flex align-items-center gap-3 flex-wrap">
					<Logo tid={wallet.tid} size={32} />
					<div style={{ minWidth: 0 }}>
						<div className="text-body-secondary small lh-1 text-truncate">
							{teamName(wallet.tid)}
						</div>
						<div className="fs-3 fw-bold lh-1 mt-1">
							{formatSportsbookMoneyFull(wallet.balance)}
						</div>
					</div>
					<div className="ms-sm-auto small text-body-secondary">
						{wallet.bets.length > 0 ? (
							<div>
								{wallet.bets.length}{" "}
								{helpers.plural("open bet", wallet.bets.length)} ·{" "}
								{formatSportsbookMoney(atRisk)} at risk
							</div>
						) : null}
						{settledWon + settledLost > 0 ? (
							<div title="Your most recent settled bets">
								{settledWon}-{settledLost} settled ·{" "}
								<span
									className={
										net > 0
											? "text-success"
											: net < 0
												? "text-danger"
												: undefined
									}
								>
									{net >= 0 ? "+" : "−"}
									{formatSportsbookMoney(Math.abs(net))}
								</span>
							</div>
						) : null}
					</div>
				</div>
				{balances.length > 1 ? (
					<div className="d-flex flex-wrap gap-2 mt-3">
						{balances.map((b) => (
							<span
								key={b.tid}
								className={`badge d-inline-flex align-items-center gap-1 ${b.tid === wallet.tid ? "text-bg-primary" : "text-bg-secondary"}`}
								style={{ fontWeight: 400 }}
								title={teamName(b.tid)}
							>
								<Logo tid={b.tid} size={14} />
								{teamAbbrev(b.tid)} {formatSportsbookMoneyFull(b.balance)}
							</span>
						))}
					</div>
				) : null}
			</div>

			<ul className="nav nav-tabs mb-3 flex-nowrap overflow-x-auto">
				{(
					[
						["games", "Games"],
						["futures", "Futures"],
						["awards", "Awards"],
						[
							"mybets",
							`My Bets${wallet.bets.length > 0 ? ` (${wallet.bets.length})` : ""}`,
						],
						["leaguebets", "League Bets"],
					] as const
				).map(([key, label]) => (
					<li className="nav-item" key={key}>
						<button
							className={`nav-link text-nowrap ${tab === key ? "active" : ""}`}
							onClick={() => setTab(key as typeof tab)}
						>
							{label}
						</button>
					</li>
				))}
			</ul>

			<div className="row">
				<div className="col-lg-8 col-xl-9">
					{tab === "games"
						? gamesTab
						: tab === "futures"
							? futuresTab
							: tab === "awards"
								? awardsTab
								: tab === "leaguebets"
									? leagueBetsTab
									: myBetsTab}
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

			<p className="text-body-secondary small mt-3">
				Play money — completely separate from the real game. Every preseason
				each team gets {formatSportsbookMoney(1_000_000)} more.
			</p>
		</>
	);
};

export default Sportsbook;
