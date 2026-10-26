import type { LineupPic } from "../../worker/util/lineupPictures.ts";
import { PlayerPicture } from "./PlayerPicture.tsx";
import { helpers } from "../util/helpers.ts";

// A FIVE, PICTURED: each man's head and shoulders in his uniform, side by
// side, his name on a tag across the bottom of him, each a link to him.

const W = 58;
// The face drawn this much wider than the card, so his head and shoulders
// fill it.
const ZOOM = 1.08;

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
	const tag = colors?.[0] ?? "#552583";
	return (
		<div className="d-flex" style={{ gap: Math.round(2 * scale) }}>
			{pids.map((pid) => {
				const pic = pics?.[pid];
				return (
					<a
						key={pid}
						href={helpers.leagueUrl(["player", pid])}
						className="position-relative d-block flex-shrink-0 text-decoration-none"
						style={{ width: w, height: h, overflow: "hidden" }}
						title={pic?.name}
					>
						<div
							style={{
								width: inner,
								height: inner * 1.5,
								marginLeft: (w - inner) / 2,
								marginTop: -inner * 0.04,
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
						<span
							className="position-absolute text-white text-truncate text-center"
							style={{
								left: 2,
								right: 2,
								bottom: 2,
								fontSize: Math.max(9, Math.round(10 * scale)),
								fontWeight: 700,
								lineHeight: 1.35,
								background: tag,
								border: "1px solid rgba(255,255,255,0.85)",
								borderRadius: 3,
							}}
						>
							{pic?.name ?? "?"}
						</span>
					</a>
				);
			})}
		</div>
	);
};
