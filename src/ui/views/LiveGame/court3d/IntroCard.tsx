import { useEffect, useRef } from "react";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import { lightness } from "./figure.ts";

// THE NAME CARD for each starter as he is called out (see intro.ts): low on
// the left, the way the broadcasts put it up - his number on his team's
// color, his name, his position and height - sliding in with each name. And
// the title over the picture as the lights go down.

export type IntroCardInfo = {
	name: string;
	jerseyNumber?: string;
	pos?: string;
	// Inches.
	hgt?: number;
	colors?: [string, string, string];
	imgURL?: string;
	imgURLSmall?: string;
	abbrev?: string;
};

const FONT = "system-ui, -apple-system, Segoe UI, Roboto, sans-serif";

const heightOf = (inches: number | undefined): string | undefined => {
	if (inches === undefined || !Number.isFinite(inches) || inches <= 0) {
		return undefined;
	}
	const r = Math.round(inches);
	return `${Math.floor(r / 12)}'${r % 12}"`;
};

const splitName = (name: string): [string, string] => {
	const n = name.trim();
	const i = n.indexOf(" ");
	return i < 0 ? ["", n] : [n.slice(0, i), n.slice(i + 1)];
};

export const IntroCard = ({
	info,
	callKey,
}: {
	info: IntroCardInfo;
	// Changes with each name called, for the card to slide in again.
	callKey: number;
}) => {
	const ref = useRef<HTMLDivElement | null>(null);
	useEffect(() => {
		ref.current?.animate?.(
			[
				{ opacity: 0, transform: "translateX(-1.4em)" },
				{ opacity: 1, transform: "translateX(0)" },
			],
			{ duration: 260, easing: "cubic-bezier(.2,.8,.2,1)" },
		);
	}, [callKey]);
	const main = info.colors?.[0] ?? "#3a3a44";
	const accent = info.colors?.[1] ?? "#9a9aa6";
	const ink = lightness(main) > 0.62 ? "#111" : "#fff";
	const [first, last] = splitName(info.name);
	const details = [info.pos, heightOf(info.hgt)].filter(Boolean).join(" · ");
	return (
		<div
			ref={ref}
			style={{
				position: "absolute",
				left: "3.5cqw",
				bottom: "5cqw",
				display: "flex",
				alignItems: "stretch",
				fontFamily: FONT,
				fontSize: "clamp(11px, 2.1cqw, 22px)",
				lineHeight: 1,
				borderRadius: 4,
				overflow: "hidden",
				boxShadow: "0 4px 14px rgba(0,0,0,.6)",
				whiteSpace: "nowrap",
				pointerEvents: "none",
			}}
		>
			<div
				style={{
					display: "flex",
					flexDirection: "column",
					alignItems: "center",
					justifyContent: "center",
					gap: "0.25em",
					padding: "0.35em 0.6em",
					background: main,
					color: ink,
					borderRight: `0.18em solid ${accent}`,
				}}
			>
				<TeamLogoInline
					alt={info.abbrev}
					imgURL={info.imgURL}
					imgURLSmall={info.imgURLSmall}
					style={{ height: "1.5em", width: "1.5em" }}
				/>
				<span
					style={{
						fontSize: "1.7em",
						fontWeight: 900,
						fontVariantNumeric: "tabular-nums",
					}}
				>
					{info.jerseyNumber ?? ""}
				</span>
			</div>
			<div
				style={{
					display: "flex",
					flexDirection: "column",
					justifyContent: "center",
					gap: "0.22em",
					padding: "0.45em 0.9em 0.45em 0.7em",
					background: "rgba(10, 10, 14, 0.92)",
					color: "#fff",
					borderBottom: `0.16em solid ${accent}`,
				}}
			>
				{first ? (
					<span
						style={{
							fontSize: "0.72em",
							fontWeight: 600,
							letterSpacing: "0.08em",
							color: "#cfcbd6",
							textTransform: "uppercase",
						}}
					>
						{first}
					</span>
				) : null}
				<span
					style={{
						fontSize: "1.45em",
						fontWeight: 900,
						letterSpacing: "0.02em",
						textTransform: "uppercase",
					}}
				>
					{last}
				</span>
				{details ? (
					<span
						style={{
							fontSize: "0.7em",
							fontWeight: 700,
							letterSpacing: "0.1em",
							color: "#f2c14e",
						}}
					>
						{details}
					</span>
				) : null}
			</div>
		</div>
	);
};

export const IntroTitle = ({ playoffs }: { playoffs: boolean }) => {
	const ref = useRef<HTMLDivElement | null>(null);
	useEffect(() => {
		ref.current?.animate?.(
			[
				{ opacity: 0, letterSpacing: "0.5em" },
				{ opacity: 1, letterSpacing: "0.18em" },
			],
			{ duration: 600, easing: "cubic-bezier(.2,.8,.2,1)" },
		);
	}, []);
	return (
		<div
			ref={ref}
			style={{
				position: "absolute",
				left: 0,
				right: 0,
				top: "40%",
				display: "flex",
				flexDirection: "column",
				alignItems: "center",
				gap: "0.35em",
				fontFamily: FONT,
				fontSize: "clamp(14px, 3.4cqw, 34px)",
				fontWeight: 900,
				letterSpacing: "0.18em",
				color: "#fff",
				textShadow: "0 2px 12px rgba(0,0,0,.8)",
				pointerEvents: "none",
				whiteSpace: "nowrap",
			}}
		>
			{playoffs ? (
				<span style={{ fontSize: "0.55em", color: "#f2c14e" }}>PLAYOFFS</span>
			) : null}
			<span>STARTING LINEUPS</span>
		</div>
	);
};
