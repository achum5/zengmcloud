import { useState } from "react";
import type { JerseySkinIds } from "../../../common/types.ts";
import { showNotification } from "../../util/showNotification.ts";
import { toWorker } from "../../util/toWorker.ts";
import { keepCourtPictures, parseCourt } from "../courtFile.ts";
import { keepScoreBugPictures } from "../scoreBugFile.ts";
import { parseScoreBug } from "../../../common/scoreBug.ts";
import { parseNationalBroadcasts } from "../../../common/nationalBroadcasts.ts";
import { toSkinURL } from "./JerseySkins.tsx";

// EVERY TEAM'S LOOK FROM ONE FILE: each team's home and away jerseys and
// its court, for a whole league at once. A team is found by its
// abbreviation (or one of the others it goes by, "aka"), or failing that its
// full name. A jersey is a picture
// written into the file, or a link to one hosted elsewhere (see
// setJerseySkin); a court is a court file's JSON, its pictures written in or
// linked the same way. (Football's: its stadium and its helmet and uniform,
// as the stadium page has them - see StadiumLook, HelmetLook.)
//
//   { "teamLooks": 1, "teams": [{ "abbrev": "BOS", "name": "Boston Celtics",
//     "home": "https://...", "away": "data:image/png;base64,...",
//     "court": { "floorImageURL": "https://...", "floorLines": "picture" } }] }
//
//   { "teamLooks": 1, "teams": [{ "abbrev": "GB",
//     "stadium": { "turfColor": "#0b3d91" }, "helmet": { "shell": "#203731" } }] }
//
// Any sport's: a team's score bug, its broadcast's, as a score bug file's
// JSON (see ScoreBugStyle), its pictures and fonts written in or linked -
// "scoreBug": { "width": 600, "height": 60, "pieces": [...] } (null: the
// league's). And beside the teams, the national networks' (see
// NationalBroadcast) - "national": [{ "network": "ESPN", "nights": ["Wed"],
// "scoreBug": {...} }] - in place of any the league had ([]: none).

type Entry = {
	abbrev?: unknown;
	aka?: unknown;
	name?: unknown;
	home?: unknown;
	away?: unknown;
	court?: unknown;
	stadium?: unknown;
	helmet?: unknown;
	scoreBug?: unknown;
};

export type TeamRef = {
	tid: number;
	abbrev: string;
	region: string;
	name: string;
};

const matchTeam = (teams: TeamRef[], e: Entry) => {
	const abbrevs = [e.abbrev, ...(Array.isArray(e.aka) ? e.aka : [])]
		.filter((a): a is string => typeof a === "string")
		.map((a) => a.trim().toLowerCase());
	const full = typeof e.name === "string" ? e.name.trim().toLowerCase() : "";
	return (
		abbrevs
			.map((a) => teams.find((t) => t.abbrev.toLowerCase() === a))
			.find((t) => t !== undefined) ??
		teams.find((t) => full && `${t.region} ${t.name}`.toLowerCase() === full) ??
		teams.find((t) => full && full.endsWith(` ${t.name.toLowerCase()}`))
	);
};

const isLink = (url: string) => /^https?:\/\//.test(url);

const ImportTeamLooks = ({
	teams,
	onSkins,
}: {
	teams: TeamRef[];
	onSkins: (tid: number, ids: JerseySkinIds) => void;
}) => {
	const [busy, setBusy] = useState<string>();

	const run = async (file: File) => {
		let entries: Entry[] | undefined;
		let national: unknown;
		try {
			const raw = JSON.parse(await file.text());
			national = raw?.national;
			entries = Array.isArray(raw?.teams)
				? raw.teams
				: national !== undefined
					? []
					: undefined;
		} catch {}
		if (!entries) {
			showNotification({
				type: "error",
				text: "That isn't a team looks file.",
			});
			return;
		}
		let done = 0;
		const missed: string[] = [];
		const problems: string[] = [];
		for (const [i, e] of entries.entries()) {
			setBusy(`${i + 1}/${entries.length}`);
			const label = String(e.abbrev ?? e.name ?? i + 1);
			const t = matchTeam(teams, e);
			if (!t) {
				missed.push(label);
				continue;
			}
			try {
				const ids: JerseySkinIds = {};
				for (const side of ["home", "away"] as const) {
					const url = e[side];
					if (typeof url !== "string") {
						continue;
					}
					const id = await toWorker("main", "setJerseySkin", {
						tid: t.tid,
						side,
						url: isLink(url) ? url : await toSkinURL(url),
					});
					if (id !== undefined) {
						ids[side] = id;
					}
				}
				if (ids.home !== undefined || ids.away !== undefined) {
					onSkins(t.tid, ids);
				}
				if (e.court !== undefined) {
					const parsed = parseCourt(JSON.stringify(e.court));
					if (typeof parsed === "string") {
						throw new Error(`court: ${parsed}`);
					}
					const kept = await keepCourtPictures(parsed);
					await toWorker("main", "updateTeamCourt", {
						tid: t.tid,
						court: kept.court,
						uploaded: kept.ids,
					});
				}
				if (e.scoreBug !== undefined) {
					const parsed = e.scoreBug === null ? null : parseScoreBug(e.scoreBug);
					if (typeof parsed === "string") {
						throw new Error(`score bug: ${parsed}`);
					}
					const kept = parsed && (await keepScoreBugPictures(parsed));
					await toWorker("main", "updateScoreBug", {
						tid: t.tid,
						bug: kept ? kept.bug : null,
						uploaded: kept?.ids,
					});
				}
				if (e.stadium !== undefined || e.helmet !== undefined) {
					await toWorker("main", "updateTeamStadium", {
						tid: t.tid,
						...(e.stadium !== undefined ? { stadium: e.stadium } : {}),
						...(e.helmet !== undefined ? { helmet: e.helmet } : {}),
					});
				}
				done += 1;
			} catch (error) {
				problems.push(`${label}: ${(error as Error).message}`);
			}
		}
		let networks: string[] | undefined;
		if (national !== undefined) {
			try {
				const parsed = parseNationalBroadcasts(national);
				if (typeof parsed === "string") {
					throw new Error(parsed);
				}
				const uploaded: string[] = [];
				const broadcasts = [];
				for (const n of parsed) {
					const kept = await keepScoreBugPictures(n.scoreBug);
					uploaded.push(...kept.ids);
					broadcasts.push({ ...n, scoreBug: kept.bug });
				}
				await toWorker("main", "updateNationalBroadcasts", {
					broadcasts,
					uploaded,
				});
				networks = [...new Set(parsed.map((n) => n.network))];
			} catch (error) {
				problems.push(`National: ${(error as Error).message}`);
			}
		}
		setBusy(undefined);
		const notes = [
			...(entries.length > 0 || networks === undefined
				? [`Updated ${done} team${done === 1 ? "" : "s"}.`]
				: []),
			...(networks !== undefined
				? [
						networks.length > 0
							? `National: ${networks.join(", ")}.`
							: "No national networks.",
					]
				: []),
			...(missed.length > 0 ? [`No team for ${missed.join(", ")}.`] : []),
			...problems,
		];
		showNotification({
			type: missed.length + problems.length > 0 ? "error" : "success",
			text: notes.join(" "),
			persistent: missed.length + problems.length > 0,
		});
	};

	return (
		<label
			className={`btn btn-light-bordered mb-0${busy !== undefined ? " disabled" : ""}`}
		>
			{busy !== undefined ? `Importing ${busy}` : "Import team looks"}
			<input
				type="file"
				accept=".json,application/json"
				hidden
				disabled={busy !== undefined}
				onChange={(event) => {
					const file = event.target.files?.[0];
					event.target.value = "";
					if (file) {
						void run(file);
					}
				}}
			/>
		</label>
	);
};

export default ImportTeamLooks;
