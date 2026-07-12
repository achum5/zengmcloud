import type { ReactNode } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import type { View } from "../../common/types.ts";
import { useLocal } from "../util/local.ts";
import {
	formatAmerican,
	formatSportsbookMoney,
} from "../../common/sportsbook.ts";

const OddsPill = ({ odds }: { odds: number }) => (
	<span
		className={`badge rounded-pill ${odds < 0 ? "text-bg-primary" : "text-bg-success"}`}
		style={{ minWidth: 52 }}
	>
		{formatAmerican(odds)}
	</span>
);

const Sportsbook = ({
	board,
	wallets,
}: View<"sportsbook">) => {
	useTitleBar({ title: "Sportsbook" });

	const { teamInfoCache } = useLocal(["teamInfoCache"]);

	const teamName = (tid: number) => {
		const t = teamInfoCache[tid];
		return t ? `${t.region} ${t.name}` : "?";
	};
	const teamAbbrev = (tid: number) => teamInfoCache[tid]?.abbrev ?? "???";

	const teamLink = (tid: number, abbrev: string) => (
		<a href={helpers.leagueUrl(["roster", `${abbrev}_${tid}`])}>{abbrev}</a>
	);

	return (
		<>
			<div className="d-flex flex-wrap gap-3 mb-3">
				{wallets.map((w) => (
					<div key={w.tid} className="card" style={{ minWidth: 220 }}>
						<div className="card-body py-2">
							<div className="text-body-secondary small">
								{teamName(w.tid)} balance
							</div>
							<div className="h4 mb-0">
								{formatSportsbookMoney(w.balance)}
							</div>
							{w.bets.length > 0 ? (
								<div className="small text-body-secondary">
									{w.bets.length} open bet{w.bets.length === 1 ? "" : "s"}
								</div>
							) : null}
						</div>
					</div>
				))}
			</div>

			<p className="text-body-secondary small">
				Play money — completely separate from the real game. Every preseason each
				team gets{" "}
				{formatSportsbookMoney(1_000_000)} more.
			</p>

			{board.games.length > 0 ? (
				<>
					<h2 className="h5 mt-3">Upcoming Games</h2>
					<div className="table-responsive">
						<table className="table table-sm align-middle">
							<thead>
								<tr>
									<th>Matchup</th>
									<th className="text-center">Moneyline</th>
									<th className="text-center">Spread</th>
									<th className="text-center">Total</th>
								</tr>
							</thead>
							<tbody>
								{board.games.map((game) => (
									<tr key={game.gid}>
										<td>
											<div>
												{teamLink(game.away.tid, game.away.abbrev)} @{" "}
												{teamLink(game.home.tid, game.home.abbrev)}
											</div>
										</td>
										<td className="text-center text-nowrap">
											<div className="d-flex flex-column gap-1 align-items-center">
												<span>
													{game.away.abbrev} <OddsPill odds={game.moneyline.away} />
												</span>
												<span>
													{game.home.abbrev} <OddsPill odds={game.moneyline.home} />
												</span>
											</div>
										</td>
										<td className="text-center text-nowrap">
											<div className="d-flex flex-column gap-1 align-items-center">
												<span>
													{game.away.abbrev}{" "}
													{-game.spread.line > 0 ? "+" : ""}
													{-game.spread.line} <OddsPill odds={game.spread.away} />
												</span>
												<span>
													{game.home.abbrev}{" "}
													{game.spread.line > 0 ? "+" : ""}
													{game.spread.line} <OddsPill odds={game.spread.home} />
												</span>
											</div>
										</td>
										<td className="text-center text-nowrap">
											<div className="d-flex flex-column gap-1 align-items-center">
												<span>
													O {game.total.line} <OddsPill odds={game.total.over} />
												</span>
												<span>
													U {game.total.line} <OddsPill odds={game.total.under} />
												</span>
											</div>
										</td>
									</tr>
								))}
							</tbody>
						</table>
					</div>
				</>
			) : null}

			<h2 className="h5 mt-4">Championship</h2>
			<FuturesList
				rows={board.championship.map((t) => ({
					key: t.tid,
					label: teamLink(t.tid, t.abbrev),
					odds: t.americanOdds,
				}))}
			/>

			<div className="row">
				<div className="col-lg-6">
					<h2 className="h5 mt-4">Conference Winners</h2>
					{board.conferences.map((conf) => (
						<div key={conf.cid} className="mb-3">
							<div className="fw-bold small">{conf.name}</div>
							<FuturesList
								rows={conf.teams.map((t) => ({
									key: t.tid,
									label: teamLink(t.tid, t.abbrev),
									odds: t.americanOdds,
								}))}
							/>
						</div>
					))}
				</div>
				<div className="col-lg-6">
					<h2 className="h5 mt-4">Division Winners</h2>
					{board.divisions.map((div) => (
						<div key={div.did} className="mb-3">
							<div className="fw-bold small">{div.name}</div>
							<FuturesList
								rows={div.teams.map((t) => ({
									key: t.tid,
									label: teamLink(t.tid, t.abbrev),
									odds: t.americanOdds,
								}))}
							/>
						</div>
					))}
				</div>
			</div>

			<h2 className="h5 mt-4">Season Win Totals</h2>
			<div className="table-responsive">
				<table className="table table-sm align-middle" style={{ maxWidth: 520 }}>
					<thead>
						<tr>
							<th>Team</th>
							<th className="text-end">Line</th>
							<th className="text-center">Over</th>
							<th className="text-center">Under</th>
						</tr>
					</thead>
					<tbody>
						{board.winTotals.map((t) => (
							<tr key={t.tid}>
								<td>{teamLink(t.tid, t.abbrev)}</td>
								<td className="text-end">{t.line}</td>
								<td className="text-center">
									<OddsPill odds={t.over} />
								</td>
								<td className="text-center">
									<OddsPill odds={t.under} />
								</td>
							</tr>
						))}
					</tbody>
				</table>
			</div>

			<h2 className="h5 mt-4">Award Futures</h2>
			<div className="row">
				{board.awards.map((race) => (
					<div key={race.award} className="col-lg-6 col-xl-4 mb-3">
						<div className="fw-bold small">{race.name}</div>
						<FuturesList
							rows={race.candidates.map((c) => ({
								key: c.pid,
								label: (
									<a href={helpers.leagueUrl(["player", c.pid])}>{c.name}</a>
								),
								sub: teamAbbrev(c.tid),
								odds: c.americanOdds,
							}))}
						/>
					</div>
				))}
			</div>
		</>
	);
};

const FuturesList = ({
	rows,
}: {
	rows: {
		key: number | string;
		label: ReactNode;
		sub?: string;
		odds: number;
	}[];
}) => (
	<ul className="list-group">
		{rows.map((row) => (
			<li
				key={row.key}
				className="list-group-item d-flex justify-content-between align-items-center py-1 px-2"
			>
				<span>
					{row.label}
					{row.sub ? (
						<span className="text-body-secondary small ms-1">{row.sub}</span>
					) : null}
				</span>
				<OddsPill odds={row.odds} />
			</li>
		))}
	</ul>
);

export default Sportsbook;
