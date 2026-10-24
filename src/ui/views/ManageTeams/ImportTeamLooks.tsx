import { useState } from "react";
import type { JerseySkinIds } from "../../../common/types.ts";
import { showNotification } from "../../util/showNotification.ts";
import { toWorker } from "../../util/toWorker.ts";
import { keepCourtPictures, parseCourt } from "../courtFile.ts";
import { toSkinURL } from "./JerseySkins.tsx";

// EVERY TEAM'S LOOK FROM ONE FILE: each team's home and away jerseys and
// its court, for a whole league at once. A team is found by its
// abbreviation (or one of the others it goes by, "aka"), or failing that its
// full name. A jersey is a picture
// written into the file, or a link to one hosted elsewhere (see
// setJerseySkin); a court is a court file's JSON, its pictures written in or
// linked the same way.
//
//   { "teamLooks": 1, "teams": [{ "abbrev": "BOS", "name": "Boston Celtics",
//     "home": "https://...", "away": "data:image/png;base64,...",
//     "court": { "floorImageURL": "https://...", "floorLines": "picture" } }] }

type Entry = {
	abbrev?: unknown;
	aka?: unknown;
	name?: unknown;
	home?: unknown;
	away?: unknown;
	court?: unknown;
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
		try {
			const raw = JSON.parse(await file.text());
			entries = Array.isArray(raw?.teams) ? raw.teams : undefined;
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
				done += 1;
			} catch (error) {
				problems.push(`${label}: ${(error as Error).message}`);
			}
		}
		setBusy(undefined);
		const notes = [
			`Updated ${done} team${done === 1 ? "" : "s"}.`,
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
