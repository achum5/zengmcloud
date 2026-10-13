import { useMemo } from "react";
import { elapsedOf, lengthsOf, momentsOf } from "./winProb.ts";

// Each team's chance of winning as the game stands (see winProb.ts), and how
// it has swung so far: above the middle line the home team's way, below it
// the visitors'.
const WinProbability = ({
	events,
	cursor,
	boxScore,
}: {
	events: any[] | undefined;
	cursor: number;
	boxScore: any;
}) => {
	const numPeriods: number = boxScore?.numPeriods ?? 4;
	const neutral = !!boxScore?.neutralSite;
	const moments = useMemo(
		() =>
			events
				? momentsOf(events, { numPeriods, ...(neutral ? { edge: 0 } : {}) })
				: [],
		[events, numPeriods, neutral],
	);
	const lengths = useMemo(
		() => lengthsOf(events ?? [], numPeriods),
		[events, numPeriods],
	);
	const teams: any[] = Array.isArray(boxScore?.teams) ? boxScore.teams : [];
	const home = teams[0];
	const away = teams[1];
	if (moments.length === 0 || !home || !away) {
		return null;
	}
	const at = Math.max(0, Math.min(cursor, moments.length - 1));
	const p = moments[at]!.p0;
	const span = Math.max(1, elapsedOf(moments[at]!, lengths, numPeriods));
	const pts: string[] = [];
	let lastX = -1;
	for (let i = 0; i <= at; i++) {
		const m = moments[i]!;
		const x = (Math.max(0, elapsedOf(m, lengths, numPeriods)) / span) * 100;
		if (x - lastX < 0.25 && i !== at) {
			continue;
		}
		lastX = x;
		pts.push(`${x.toFixed(2)},${(36 - m.p0 * 36).toFixed(2)}`);
	}
	const line = pts.join(" ");
	const first = pts[0] ?? "0,18";
	const last = pts.at(-1) ?? "0,18";
	const area = `M${first.split(",")[0]},18 L${line.replaceAll(" ", " L")} L${last.split(",")[0]},18 Z`;
	const homeColor: string = home.colors?.[0] ?? "#888";
	const awayColor: string = away.colors?.[0] ?? "#555";
	const pct = (x: number) => `${Math.round(x * 100)}%`;
	return (
		<div className="mb-2" title="Chance of winning">
			<div className="d-flex align-items-center gap-2 small">
				<span className="text-nowrap">
					{away.abbrev} {pct(1 - p)}
				</span>
				<div
					className="flex-grow-1 d-flex rounded overflow-hidden"
					style={{ height: 6 }}
				>
					<div style={{ width: pct(1 - p), background: awayColor }} />
					<div style={{ width: pct(p), background: homeColor }} />
				</div>
				<span className="text-nowrap">
					{pct(p)} {home.abbrev}
				</span>
			</div>
			<svg
				viewBox="0 0 100 36"
				preserveAspectRatio="none"
				width="100%"
				height="36"
				aria-hidden="true"
			>
				<defs>
					<clipPath id="wp-top">
						<rect x="0" y="0" width="100" height="18" />
					</clipPath>
					<clipPath id="wp-bottom">
						<rect x="0" y="18" width="100" height="18" />
					</clipPath>
				</defs>
				<path d={area} fill={homeColor} opacity={0.8} clipPath="url(#wp-top)" />
				<path
					d={area}
					fill={awayColor}
					opacity={0.8}
					clipPath="url(#wp-bottom)"
				/>
				<line
					x1="0"
					y1="18"
					x2="100"
					y2="18"
					stroke="currentColor"
					strokeOpacity={0.3}
					strokeWidth={0.5}
					vectorEffect="non-scaling-stroke"
				/>
			</svg>
		</div>
	);
};

export default WinProbability;
