import type { LineupPic } from "../../worker/util/lineupPictures.ts";
import { PlayerPicture } from "./PlayerPicture.tsx";
import { helpers } from "../util/helpers.ts";

// A FIVE, PICTURED: each man's head and shoulders in his uniform, side by
// side on a wash of his team's color, his name under him, each a link to him.

const W = 58;
// The face drawn this much wider than its frame, so he fills it.
const ZOOM = 1.06;

const alpha = (hex: string, a: string) =>
	/^#[\da-f]{6}$/i.test(hex) ? `${hex}${a}` : hex;

export const LineupFaces = ({
	pids,
	pics,
	colors,
	jersey,
	scale = 1,
}: {
	pids: number[];
	pics: Record<number, LineupPic> | undefined;
	colors?: [string, string, string];
	jersey?: string;
	scale?: number;
}) => {
	const w = Math.round(W * scale);
	const h = Math.round(w * 1.42);
	const inner = w * ZOOM;
	const c = colors?.[0] ?? "#552583";
	return (
		<div className="d-flex" style={{ gap: Math.round(4 * scale) }}>
			{pids.map((pid) => {
				const pic = pics?.[pid];
				return (
					<a
						key={pid}
						href={helpers.leagueUrl(["player", pid])}
						className="d-block flex-shrink-0 text-decoration-none text-body"
						style={{ width: w }}
						title={pic?.name}
					>
						<div
							style={{
								height: h,
								overflow: "hidden",
								borderRadius: 8,
								background: `linear-gradient(180deg, ${alpha(c, "14")} 0%, ${alpha(c, "40")} 100%)`,
								boxShadow: `inset 0 -2px 0 ${alpha(c, "cc")}`,
							}}
						>
							<div
								style={{
									width: inner,
									height: inner * 1.5,
									marginLeft: (w - inner) / 2,
									marginTop: -inner * 0.02,
								}}
							>
								{pic ? (
									<PlayerPicture
										face={pic.face}
										imgURL={pic.imgURL}
										colors={colors}
										jersey={jersey}
										lazy
									/>
								) : null}
							</div>
						</div>
						<div
							className="text-truncate text-center"
							style={{
								fontSize: Math.max(10, Math.round(11 * scale)),
								fontWeight: 600,
								lineHeight: 1.2,
								marginTop: 3,
							}}
						>
							{pic?.name ?? "?"}
						</div>
					</a>
				);
			})}
		</div>
	);
};
