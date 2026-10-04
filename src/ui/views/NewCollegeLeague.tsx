import { useId, useMemo, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { useLocal } from "../util/local.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { showNotification } from "../util/showNotification.ts";
import { ProgressBarText } from "../components/ProgressBarText.tsx";
import { choice } from "../../common/random.ts";
import type { View } from "../../common/types.ts";

// New college league: pick your school, name the league, go. Every D1 program
// is in it; the rules (halves, five fouls, NIL instead of salaries) are set
// automatically.

const stars = (prestige: number) => {
	const n = Math.max(1, Math.min(5, Math.round(prestige / 20)));
	return "★".repeat(n) + "☆".repeat(5 - n);
};

const NewCollegeLeague = ({ conferences }: View<"newCollegeLeague">) => {
	useTitleBar({ title: "New College League", hideNewWindow: true });

	const allTeams = useMemo(
		() => conferences.flatMap((conf) => conf.teams),
		[conferences],
	);

	const [name, setName] = useState("College League");
	const [tid, setTid] = useState(
		() =>
			allTeams.find((t) => t.abbrev === "LEX")?.tid ?? allTeams[0]?.tid ?? 0,
	);
	const [creating, setCreating] = useState(false);

	const leagueCreationID = useId();
	const { leagueCreation, leagueCreationPercent } = useLocal([
		"leagueCreation",
		"leagueCreationPercent",
	]);

	const team = allTeams.find((t) => t.tid === tid);
	const conference = conferences.find((conf) =>
		conf.teams.some((t) => t.tid === tid),
	);

	const create = async () => {
		setCreating(true);
		try {
			const lid = await toWorker("main", "createCollegeLeague", {
				name: name.trim() || "College League",
				tid,
				leagueCreationID,
			});
			realtimeUpdate([], `/l/${lid}`);
		} catch (error) {
			console.error(error);
			setCreating(false);
			showNotification({
				type: "error",
				text: error.message,
				persistent: true,
			});
		}
	};

	return (
		<div style={{ maxWidth: 520 }}>
			<div className="mb-3">
				<label className="form-label" htmlFor="college-league-name">
					League name
				</label>
				<input
					id="college-league-name"
					className="form-control"
					value={name}
					onChange={(event) => setName(event.target.value)}
				/>
			</div>

			<div className="mb-3">
				<label className="form-label" htmlFor="college-league-team">
					Your school
				</label>
				<div className="input-group">
					<select
						id="college-league-team"
						className="form-select"
						value={tid}
						onChange={(event) => setTid(Number(event.target.value))}
					>
						{conferences.map((conf) => (
							<optgroup key={conf.cid} label={conf.name}>
								{conf.teams.map((t) => (
									<option key={t.tid} value={t.tid}>
										{t.region} {t.name}
									</option>
								))}
							</optgroup>
						))}
					</select>
					<button
						type="button"
						className="btn btn-light-bordered"
						onClick={() => setTid(choice(allTeams).tid)}
					>
						Random
					</button>
				</div>
			</div>

			{team ? (
				<div className="card mb-3">
					<div className="card-body d-flex align-items-center gap-3">
						<div
							className="rounded d-flex align-items-center justify-content-center fw-bold flex-shrink-0"
							style={{
								width: 56,
								height: 56,
								backgroundColor: team.colors[0],
								color: team.colors[1],
								fontSize: team.abbrev.length > 3 ? "1rem" : "1.25rem",
							}}
						>
							{team.abbrev}
						</div>
						<div>
							<div className="fw-bold">
								{team.region} {team.name}
							</div>
							<div className="text-body-secondary small">
								{conference?.name}
							</div>
							<div className="text-warning" title="Program prestige">
								{stars(team.prestige)}
							</div>
						</div>
					</div>
				</div>
			) : null}

			<button
				type="button"
				className="btn btn-lg btn-primary"
				disabled={creating}
				onClick={() => void create()}
			>
				{creating ? "Creating…" : "Create league"}
			</button>

			{creating &&
			(leagueCreationPercent?.id === leagueCreationID ||
				leagueCreation?.id === leagueCreationID) ? (
				<div className="mt-3">
					<ProgressBarText
						text={leagueCreation?.status ?? ""}
						percent={leagueCreationPercent?.percent ?? 0}
					/>
				</div>
			) : null}
		</div>
	);
};

export default NewCollegeLeague;
