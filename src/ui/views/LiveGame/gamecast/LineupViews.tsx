import clsx from "clsx";
import { useMemo, useState, type ReactNode } from "react";
import { helpers } from "../../../util/helpers.ts";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import { teamsInDisplayOrder } from "../../../util/boxScoreDisplayOrder.ts";
import { useLocal } from "../../../util/local.ts";
import { usePlayerFace } from "../../../util/playerFaces.ts";
import { PlayerPicture } from "../../../components/PlayerPicture.tsx";
import {
	brandVars,
	useAllTeamColors,
} from "../../../components/TeamBanner.tsx";
import { Headshot, Switch } from "./Gamecast.tsx";
import {
	lineupsOf,
	onOffOf,
	poss,
	stintsOf,
	type Stints,
	type Tot,
} from "./stints.ts";

// ROTATIONS, LINEUPS, ON/OFF - from who was on the floor (see stints.ts).

const lastName = (name: string) => {
	const words = name.trim().split(/\s+/);
	return words.length > 1 ? words.slice(1).join(" ") : name;
};
const shortName = (name: string) => {
	const words = name.trim().split(/\s+/);
	return words.length > 1
		? `${words[0]![0]}. ${words.slice(1).join(" ")}`
		: name;
};
const clock = (secs: number) => {
	const s = Math.round(secs);
	return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
};
const rating = (pts: number, p: number) =>
	p > 0 ? (100 * pts) / p : undefined;
const fmt = (v: number | undefined, d = 1) =>
	v === undefined || !Number.isFinite(v) ? "-" : v.toFixed(d);
const signed = (v: number | undefined, d = 1) =>
	v === undefined || !Number.isFinite(v)
		? "-"
		: `${v > 0 ? "+" : ""}${v.toFixed(d)}`;
const tone = (v: number | undefined) =>
	v === undefined || !Number.isFinite(v) || Math.abs(v) < 0.05
		? undefined
		: v > 0
			? "lv-good"
			: "lv-bad";

// Which of the stints' two sides a team is.
const sideOf = (stints: Stints, tid: number) =>
	(stints.tids[0] === tid ? 0 : 1) as 0 | 1;

const TeamHead = ({ t, children }: { t: any; children: ReactNode }) => (
	<tr className="bsp-head">
		<th className="bsp-name">
			<span className="bsp-team">
				<TeamLogoInline
					imgURL={t.imgURL}
					imgURLSmall={t.imgURLSmall}
					size={16}
				/>
				{(t.region || t.name || "").toUpperCase()}
			</span>
		</th>
		{children}
	</tr>
);

// ROTATIONS: each man's stretches on the floor across the game, each bar in
// green or red by how his team did over it (grey: even).
export const Rotations = ({
	boxScore,
	stints,
}: {
	boxScore: any;
	stints: Stints;
}) => {
	const total = stints.end || 1;
	const pctOf = (s: number) => `${(100 * s) / total}%`;
	const numStarters = boxScore.numPlayersOnCourt ?? 5;
	const labels = stints.periods.map((_, i) =>
		i < (boxScore.numPeriods ?? 4)
			? `Q${i + 1}`
			: `OT${i - (boxScore.numPeriods ?? 4) + 1}`,
	);
	const lines = (
		<div className="rot-lines">
			{stints.periods.slice(1).map(([a], i) => (
				<span key={i} style={{ left: pctOf(a) }} />
			))}
		</div>
	);
	const periodHeads = (
		<th className="rot-track rot-heads">
			{stints.periods.map(([a, b], i) => (
				<span key={i} style={{ left: pctOf(a), width: pctOf(b - a) }}>
					{labels[i]}
				</span>
			))}
		</th>
	);
	return (
		<>
			{teamsInDisplayOrder(boxScore.teams as [any, any]).map((t: any) => {
				const side = sideOf(stints, t.tid);
				const played = t.players.filter((p: any) => p.min > 0);
				const starters = played
					.filter((p: any) => p.gs === 1)
					.slice(0, numStarters);
				const bench = played.filter((p: any) => !starters.includes(p));
				const row = (p: any) => (
					<tr key={p.pid}>
						<th className="bsp-name">
							<a href={helpers.leagueUrl(["player", p.pid])}>
								<Headshot pid={p.pid} season={boxScore.season} size={24} />
								<span className="bsp-pname">{shortName(p.name)}</span>
							</a>
						</th>
						<td className="rot-track">
							{lines}
							{stintsOf(stints, side, p.pid).map((s, i) => (
								<span
									key={i}
									className={clsx("rot-bar", {
										"rot-up": s.margin > 0,
										"rot-down": s.margin < 0,
									})}
									style={{
										left: pctOf(s.start),
										width: pctOf(s.end - s.start),
									}}
									title={`${clock(s.end - s.start)}, ${s.margin > 0 ? "+" : ""}${s.margin}`}
								/>
							))}
						</td>
					</tr>
				);
				return (
					<div className="bsp-wrap" key={t.tid}>
						<table className="bsp-table rot-table">
							<thead>
								<TeamHead t={t}>{periodHeads}</TeamHead>
							</thead>
							<tbody>
								{starters.map(row)}
								{bench.length > 0 ? (
									<tr className="bsp-head bsp-bench">
										<th className="bsp-name">BENCH</th>
										{periodHeads}
									</tr>
								) : null}
								{bench.map(row)}
							</tbody>
						</table>
					</div>
				);
			})}
		</>
	);
};

// A man's head and shoulders, cut out (a drawn face: no ground behind it),
// for a lineup's men stood shoulder to shoulder, each a little in front of
// the one to his left. A drawn face is 400 x 600, his hair from about 55:
// the bust from BUST_TOP down to the foot. A photo fills the box from its top.
const BUST_TOP = 40;
const Bust = ({ pid, season }: { pid: number; season: number }) => {
	const { lid } = useLocal(["lid"]);
	const face = usePlayerFace(pid, season, lid);
	const photo = !!face?.imgURL;
	return (
		<div className={clsx("lv-bust", { "lv-bust-photo": photo })}>
			{face && (face.face || face.imgURL) ? (
				<div
					className="lv-bust-in"
					style={
						photo ? undefined : { marginTop: `${(-100 * BUST_TOP) / 400}%` }
					}
				>
					<PlayerPicture
						face={face.face}
						imgURL={face.imgURL}
						colors={face.colors}
						jersey={face.jersey}
						lazy
					/>
				</div>
			) : null}
		</div>
	);
};

// LINEUPS: every group of a team's men on the floor together - all five, or
// four, three, two of them - with their time, possessions and ratings.
const SIZES = [5, 4, 3, 2] as const;
export const Lineups = ({
	boxScore,
	stints,
}: {
	boxScore: any;
	stints: Stints;
}) => {
	const [k, setK] = useState<number>(5);
	const allColors = useAllTeamColors();
	const names = useMemo(() => {
		const m = new Map<number, string>();
		for (const t of boxScore.teams) {
			for (const p of t.players) {
				m.set(p.pid, p.name);
			}
		}
		return m;
	}, [boxScore]);
	// (Short ones left out: a five together under 2:30, a smaller group under
	// five minutes.)
	const least = k === 5 ? 150 : 300;
	return (
		<div className="lv">
			<div className="bsp-views lv-views">
				<Switch
					options={SIZES.map((n) => [n, `${n}-man`] as [number, string])}
					value={k}
					onChange={setK}
				/>
			</div>
			{teamsInDisplayOrder(boxScore.teams as [any, any]).map((t: any) => {
				const side = sideOf(stints, t.tid);
				const colors = brandVars(allColors?.[t.tid]);
				const list = lineupsOf(stints, side, k)
					.filter((l) => l.secs >= least)
					.slice(0, 12);
				return (
					<div className="bsp-wrap" key={t.tid}>
						<table className="bsp-table lv-table">
							<thead>
								<TeamHead t={t}>
									<th>MIN</th>
									<th>POSS</th>
									<th>ORTG</th>
									<th>DRTG</th>
									<th>NRTG</th>
								</TeamHead>
							</thead>
							<tbody>
								{list.length === 0 ? (
									<tr>
										<td className="lv-none" colSpan={6}>
											None that long together.
										</td>
									</tr>
								) : null}
								{list.map((l) => {
									const o = rating(l.ptsFor, l.possFor);
									const d = rating(l.ptsAgainst, l.possAgainst);
									const n =
										o !== undefined && d !== undefined ? o - d : undefined;
									return (
										<tr key={l.pids.join()}>
											<th className="bsp-name lv-faces">
												<div className="lv-busts" style={colors}>
													{l.pids.map((pid) => (
														<a
															key={pid}
															className="lv-face"
															href={helpers.leagueUrl(["player", pid])}
															title={names.get(pid)}
														>
															<Bust pid={pid} season={boxScore.season} />
															<span>{lastName(names.get(pid) ?? "")}</span>
														</a>
													))}
												</div>
											</th>
											<td>{clock(l.secs)}</td>
											<td>{Math.round(l.poss)}</td>
											<td>{fmt(o)}</td>
											<td>{fmt(d)}</td>
											<td className={tone(n)}>{signed(n)}</td>
										</tr>
									);
								})}
							</tbody>
						</table>
					</div>
				);
			})}
		</div>
	);
};

// ON/OFF: a man's team with him on the floor against with him off it - its
// own numbers, or the other team's.
const ROWS: {
	label: string;
	get: (own: Tot, opp: Tot, secs: number) => number | undefined;
	pct?: boolean;
	lowerBetter?: boolean;
	noDiff?: boolean;
}[] = [
	{ label: "MIN", get: (_o, _p, secs) => secs / 60, noDiff: true },
	{
		label: "POSS",
		get: (own, opp) => (poss(own) + poss(opp)) / 2,
		noDiff: true,
	},
	{ label: "+/-", get: (own, opp) => own.pts - opp.pts },
	{ label: "ORTG", get: (own) => rating(own.pts, poss(own)) },
	{
		label: "DRTG",
		get: (_own, opp) => rating(opp.pts, poss(opp)),
		lowerBetter: true,
	},
	{
		label: "NRTG",
		get: (own, opp) => {
			const a = rating(own.pts, poss(own));
			const b = rating(opp.pts, poss(opp));
			return a !== undefined && b !== undefined ? a - b : undefined;
		},
	},
	{
		label: "eFG%",
		get: (own) =>
			own.fga > 0 ? (100 * (own.fg + 0.5 * own.tp)) / own.fga : undefined,
		pct: true,
	},
	{
		label: "OREB%",
		get: (own, opp) =>
			own.orb + opp.drb > 0 ? (100 * own.orb) / (own.orb + opp.drb) : undefined,
		pct: true,
	},
	{
		label: "DREB%",
		get: (own, opp) =>
			own.drb + opp.orb > 0 ? (100 * own.drb) / (own.drb + opp.orb) : undefined,
		pct: true,
	},
	{
		label: "REB%",
		get: (own, opp) => {
			const all = own.orb + own.drb + opp.orb + opp.drb;
			return all > 0 ? (100 * (own.orb + own.drb)) / all : undefined;
		},
		pct: true,
	},
	{
		label: "AST%",
		get: (own) => (own.fg > 0 ? (100 * own.ast) / own.fg : undefined),
		pct: true,
	},
	{
		label: "BLK%",
		get: (own, opp) =>
			opp.fga - opp.tpa > 0 ? (100 * own.blk) / (opp.fga - opp.tpa) : undefined,
		pct: true,
	},
	{
		label: "STL%",
		get: (own, opp) =>
			poss(opp) > 0 ? (100 * own.stl) / poss(opp) : undefined,
		pct: true,
	},
	{
		label: "TOV%",
		get: (own) => (poss(own) > 0 ? (100 * own.tov) / poss(own) : undefined),
		pct: true,
		lowerBetter: true,
	},
];
export const OnOff = ({
	boxScore,
	stints,
}: {
	boxScore: any;
	stints: Stints;
}) => {
	const [whose, setWhose] = useState<"own" | "opp">("own");
	const teams = teamsInDisplayOrder(boxScore.teams as [any, any]);
	const options = teams.flatMap((t: any) =>
		t.players
			.filter((p: any) => p.min > 0)
			.map((p: any) => ({
				pid: p.pid,
				name: p.name,
				tid: t.tid,
				abbrev: t.abbrev,
			})),
	);
	const [pid, setPid] = useState<number | undefined>(options[0]?.pid);
	const chosen = options.find((o: any) => o.pid === pid) ?? options[0];
	if (!chosen) {
		return null;
	}
	const side = sideOf(stints, chosen.tid);
	const { on, off } = onOffOf(stints, side, chosen.pid);
	const pick = (x: typeof on) =>
		whose === "own" ? ([x.own, x.opp] as const) : ([x.opp, x.own] as const);
	return (
		<div className="lv">
			<div className="bsp-views lv-views">
				<Switch
					options={[
						["own", "Own Team"],
						["opp", "Opposing Team"],
					]}
					value={whose}
					onChange={setWhose}
				/>
				<select
					className="form-select form-select-sm lv-select"
					value={chosen.pid}
					onChange={(e) => setPid(Number(e.target.value))}
				>
					{options.map((o: any) => (
						<option key={o.pid} value={o.pid}>
							{o.abbrev} {o.name}
						</option>
					))}
				</select>
			</div>
			<div className="bsp-wrap">
				<table className="bsp-table lv-onoff">
					<thead>
						<tr className="bsp-head">
							<th className="bsp-name">STAT</th>
							<th>
								<div className="lv-who">
									<Headshot
										pid={chosen.pid}
										season={boxScore.season}
										size={36}
									/>
									<span className="lv-tag lv-on">ON</span>
								</div>
							</th>
							<th>
								<div className="lv-who lv-off-face">
									<Headshot
										pid={chosen.pid}
										season={boxScore.season}
										size={36}
									/>
									<span className="lv-tag">OFF</span>
								</div>
							</th>
							<th>DIFF</th>
						</tr>
					</thead>
					<tbody>
						{ROWS.map((r) => {
							const [a1, b1] = pick(on);
							const [a2, b2] = pick(off);
							const v1 = r.get(a1, b1, on.secs);
							const v2 = r.get(a2, b2, off.secs);
							const diff =
								r.noDiff || v1 === undefined || v2 === undefined
									? undefined
									: v1 - v2;
							// (Better or worse for his team: the other team's numbers
							// the other way round.)
							const better =
								diff === undefined
									? undefined
									: diff *
										(r.lowerBetter ? -1 : 1) *
										(whose === "opp" ? -1 : 1);
							const show = (v: number | undefined) =>
								r.label === "MIN"
									? v === undefined
										? "-"
										: String(Math.round(v))
									: r.label === "POSS" || r.label === "+/-"
										? v === undefined
											? "-"
											: r.label === "+/-"
												? signed(v, 0)
												: String(Math.round(v))
										: `${fmt(v)}${r.pct ? "%" : ""}`;
							return (
								<tr key={r.label}>
									<th className="bsp-name lv-stat">{r.label}</th>
									<td>{show(v1)}</td>
									<td>{show(v2)}</td>
									<td className={tone(better)}>
										{diff === undefined
											? "—"
											: `${signed(diff, r.label === "+/-" ? 0 : 1)}${r.pct ? "%" : ""}`}
									</td>
								</tr>
							);
						})}
					</tbody>
				</table>
			</div>
		</div>
	);
};
