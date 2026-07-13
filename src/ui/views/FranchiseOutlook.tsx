import { useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import type { View } from "../../common/types.ts";
import { TeamLogoInline } from "../components/TeamLogoInline.tsx";

type TierKey = "allIn" | "buyer" | "fringe" | "seller" | "teardown";

// Each buy/sell tier gets a label + Bootstrap badge color.
const TIER_META: Record<TierKey, { label: string; className: string }> = {
	allIn: { label: "All-in", className: "bg-danger" },
	buyer: { label: "Buyer", className: "bg-success" },
	fringe: { label: "Fringe", className: "bg-secondary" },
	seller: { label: "Seller", className: "bg-warning text-dark" },
	teardown: { label: "Teardown", className: "bg-info text-dark" },
};

const TierBadge = ({ tier }: { tier: TierKey }) => {
	const meta = TIER_META[tier];
	return <span className={`badge ${meta.className}`}>{meta.label}</span>;
};

const TABLE_CLASS = "table table-striped table-borderless table-sm align-middle";

// One player, compact: "Name o78 a26 v82.1 PG $35000/2031".
const fmtPlayer = (p: {
	name: string;
	ovr: number;
	age: number;
	value: number;
	pos: string;
	contract: number;
	exp: number;
}) =>
	`${p.name} o${p.ovr} a${p.age} v${p.value} ${p.pos} $${p.contract}/${p.exp}`;

// Build the full plain-text diagnostic dump of every team's posture + the raw
// signals behind it, for pasting into a review.
const buildDiagnostics = (view: View<"franchiseOutlook">): string => {
	const { teams, context, season, salaryCap, luxuryPayroll } = view;
	const lines: string[] = [];
	lines.push(`=== FRANCHISE OUTLOOK DIAGNOSTICS — Season ${season} ===`);
	lines.push(
		`Context: teams=${context.numActiveTeams} starterOvr=${context.starterOvr} rotationOvr=${context.rotationOvr} starValue=${context.starValue} coreValue=${context.coreValue} cap=${salaryCap} luxury=${luxuryPayroll} floor=${context.minPayroll}`,
	);
	lines.push("");

	for (const t of teams) {
		const need = t.needs.length
			? t.needs.map((n) => `${n.pos}:${n.severity}`).join(",")
			: "—";
		const surplus = t.surpluses.length
			? t.surpluses.map((s) => `${s.pos}:${s.depth}`).join(",")
			: "—";
		const meta = TIER_META[t.tier as TierKey];

		lines.push(
			`[${String(t.ovrRank).padStart(2)}] ${t.abbrev}  tier=${meta.label}  aggr=${t.aggression.toFixed(2)}`,
		);
		lines.push(
			`     record=${t.won}-${t.lost} winp=${t.winp.toFixed(3)} rankPct=${t.ovrRankPct.toFixed(2)} contention=${t.contention.toFixed(3)} | avgAge=${t.avgAge.toFixed(1)} youngCore=${t.youngCoreCount} strat=${t.strategy || "—"}`,
		);
		lines.push(
			`     seeking="${t.seeking}" starGap=${t.starGap} targetPos=${t.targetPos ?? "—"}`,
		);
		lines.push(`     needs=[${need}] surpluses=[${surplus}]`);
		lines.push(
			`     cap: payroll=${t.cap.payroll} space=${t.cap.capSpace} overCap=${t.cap.overCap} overLux=${t.cap.overLuxury} floor=${t.cap.underFloor} wantsRelief=${t.cap.wantsRelief} canAbsorb=${t.cap.canAbsorb}`,
		);
		lines.push(`     top: ${t.topPlayer ? fmtPlayer(t.topPlayer) : "—"}`);
		lines.push(
			`     core(${t.buildingBlockCount}): ${
				t.buildingBlocks.length
					? t.buildingBlocks.map(fmtPlayer).join(" | ")
					: "—"
			}`,
		);
		lines.push(
			`     shopping(${t.shopping.length}): ${
				t.shopping.length ? t.shopping.map(fmtPlayer).join(" | ") : "—"
			}`,
		);
		lines.push("");
	}

	return lines.join("\n");
};

const FranchiseOutlook = (view: View<"franchiseOutlook">) => {
	const { teams, userTid, season } = view;
	useTitleBar({ title: "Franchise Outlook" });

	const [copied, setCopied] = useState(false);
	const [fallback, setFallback] = useState<string | undefined>();

	const diagnostics = buildDiagnostics(view);

	const copy = async () => {
		setFallback(undefined);
		try {
			await navigator.clipboard.writeText(diagnostics);
			setCopied(true);
			globalThis.setTimeout(() => setCopied(false), 3000);
		} catch {
			// Clipboard blocked — drop the text into a box to select manually.
			setFallback(diagnostics);
		}
	};

	return (
		<>
			<div className="mb-3">
				<button
					className={`btn btn-sm ${copied ? "btn-success" : "btn-light-bordered"}`}
					onClick={copy}
					title="Copy a full diagnostic dump of every team's posture"
				>
					{copied ? "✓ Copied" : "Copy diagnostics"}
				</button>
			</div>

			{fallback !== undefined ? (
				<textarea
					className="form-control mb-3"
					style={{ fontFamily: "monospace", fontSize: 11 }}
					rows={10}
					readOnly
					value={fallback}
					onFocus={(event) => event.target.select()}
				/>
			) : null}

			<div style={{ overflowX: "auto" }}>
				<table className={TABLE_CLASS} style={{ minWidth: 720 }}>
					<thead>
						<tr>
							<th>Team</th>
							<th>Outlook</th>
							<th className="text-end">Record</th>
							<th>Seeking</th>
							<th>Needs</th>
							<th>Shopping</th>
							<th className="text-center">Core</th>
						</tr>
					</thead>
					<tbody>
						{teams.map((t) => {
							const isUser = t.tid === userTid;
							return (
								<tr key={t.tid} className={isUser ? "table-info" : undefined}>
									<td style={{ whiteSpace: "nowrap" }}>
										<TeamLogoInline
											imgURL={t.imgURL}
											imgURLSmall={t.imgURLSmall}
											includePlaceholderIfNoLogo
											size={20}
										/>{" "}
										<a
											href={helpers.leagueUrl([
												"roster",
												`${t.abbrev}_${t.tid}`,
												season,
											])}
											className="fw-bold"
										>
											{t.abbrev}
										</a>
										<span className="text-body-secondary ms-1 small">
											#{t.ovrRank}
										</span>
									</td>
									<td>
										<TierBadge tier={t.tier as TierKey} />
									</td>
									<td className="text-end" style={{ whiteSpace: "nowrap" }}>
										{t.won}-{t.lost}
									</td>
									<td className="small">{t.seeking}</td>
									<td>
										{t.needs.length === 0 ? (
											<span className="text-body-secondary">—</span>
										) : (
											t.needs.map((n) => (
												<span
													key={n.pos}
													className="badge bg-light-bordered text-body me-1"
												>
													{n.pos}
												</span>
											))
										)}
									</td>
									<td className="small">
										{t.shopping.length === 0 ? (
											<span className="text-body-secondary">—</span>
										) : (
											t.shopping.map((p, i) => (
												<span key={p.pid}>
													{i > 0 ? ", " : ""}
													<a href={helpers.leagueUrl(["player", p.pid])}>
														{p.name}
													</a>{" "}
													<span className="text-body-secondary">
														({p.ovr}, {p.age})
													</span>
												</span>
											))
										)}
									</td>
									<td
										className="text-center"
										title={t.buildingBlocks.map((p) => p.name).join(", ")}
									>
										{t.buildingBlockCount}
									</td>
								</tr>
							);
						})}
					</tbody>
				</table>
			</div>
		</>
	);
};

export default FranchiseOutlook;
