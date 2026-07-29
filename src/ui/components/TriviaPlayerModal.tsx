import { useEffect, useState } from "react";
import { Modal } from "./Modal.tsx";
import { PlayerPicture } from "./PlayerPicture.tsx";
import { JerseyNumber } from "./JerseyNumber.tsx";
import { Height } from "./Height.tsx";
import { toWorker } from "../util/toWorker.ts";
import { helpers } from "../util/helpers.ts";

// The player page, brought to the game instead of the other way round. Leaving
// a grid half-solved to look someone up loses the board, so a tap on a player
// you've already named opens this.

type Profile = any;

const STAT_LABELS: Record<string, string> = {
	gp: "GP",
	gs: "GS",
	min: "MP",
	pts: "PTS",
	trb: "TRB",
	ast: "AST",
	stl: "STL",
	blk: "BLK",
	tov: "TOV",
	fgp: "FG%",
	tpp: "3P%",
	ftp: "FT%",
	keyStats: "Stats",
};

// playersPlus already returns per-game numbers (it is not in totals mode), so
// these are formatted, never divided - dividing again turned a 34-minute
// starter into 0.6.
const COUNTS = new Set(["gp", "gs"]);

const formatStat = (row: any, key: string): string => {
	const value = row[key];
	if (value === undefined || value === null) {
		return "";
	}
	if (typeof value === "string") {
		return value;
	}
	return COUNTS.has(key) ? String(Math.round(value)) : value.toFixed(1);
};

export const TriviaPlayerModal = ({
	pid,
	onHide,
}: {
	pid: number | undefined;
	onHide: () => void;
}) => {
	const [profile, setProfile] = useState<Profile | undefined>();

	useEffect(() => {
		if (pid === undefined) {
			return;
		}
		let stale = false;
		setProfile(undefined);
		void toWorker("main", "triviaPlayerProfile", { pid }).then((p) => {
			if (!stale) {
				setProfile(p);
			}
		});
		return () => {
			stale = true;
		};
	}, [pid]);

	const statKeys: string[] = profile?.statKeys ?? [];

	return (
		<Modal show={pid !== undefined} onHide={onHide} size="lg" scrollable>
			<Modal.Header closeButton>
				<Modal.Title className="fs-5">{profile?.name ?? "Player"}</Modal.Title>
			</Modal.Header>
			<Modal.Body>
				{profile === undefined ? (
					<div className="text-body-secondary">Loading</div>
				) : (
					<>
						<div className="trivia-profile-head">
							<div className="trivia-profile-face">
								<PlayerPicture
									face={profile.face}
									imgURL={profile.imgURL}
									colors={profile.colors}
									jersey={profile.jersey}
								/>
							</div>
							<div className="flex-grow-1" style={{ minWidth: 0 }}>
								<div className="h5 mb-1">
									{profile.pos ? (
										<span className="text-body-secondary me-2">
											{profile.pos}
										</span>
									) : null}
									{profile.name}
								</div>
								<div className="trivia-profile-facts">
									{profile.teamName ? <div>{profile.teamName}</div> : null}
									<div>
										{profile.retiredYear !== undefined &&
										profile.retiredYear < Infinity
											? `Retired ${profile.retiredYear}`
											: `Age ${profile.age}`}
									</div>
									{profile.hgt ? (
										<div>
											<Height inches={profile.hgt} />, {profile.weight} lbs
										</div>
									) : null}
									{profile.bornLoc ? <div>{profile.bornLoc}</div> : null}
									{profile.college && profile.college !== "None" ? (
										<div>{profile.college}</div>
									) : null}
									<div>
										{profile.draft?.round > 0
											? `Drafted ${profile.draft.year}: round ${profile.draft.round}, pick ${profile.draft.pick}`
											: `Undrafted${profile.draft?.year ? ` in ${profile.draft.year}` : ""}`}
									</div>
								</div>
							</div>
							{profile.jerseyNumber ? (
								<JerseyNumber
									className="flex-shrink-0"
									number={profile.jerseyNumber}
									start={profile.draft?.year ?? 0}
									end={profile.draft?.year ?? 0}
									t={
										profile.colors
											? {
													colors: profile.colors,
													name: "",
													region: profile.teamName ?? "",
												}
											: undefined
									}
								/>
							) : null}
						</div>

						{profile.awards.length > 0 ? (
							<div className="d-flex flex-wrap gap-1 mb-3">
								{profile.hof ? (
									<span className="badge text-bg-warning">Hall of Fame</span>
								) : null}
								{profile.awards
									.filter(
										(a: any) => a.type !== "Inducted into the Hall of Fame",
									)
									.map((a: any) => (
										<span
											key={a.type}
											className="badge text-bg-secondary"
											title={a.seasons.join(", ")}
										>
											{a.count > 1 ? `${a.count}x ` : ""}
											{a.type}
										</span>
									))}
							</div>
						) : null}

						<div className="table-responsive">
							<table className="table table-striped table-borderless table-sm align-middle mb-0">
								<thead>
									<tr>
										<th>Season</th>
										<th>Team</th>
										<th className="text-end">Age</th>
										{statKeys.map((key) => (
											<th key={key} className="text-end">
												{STAT_LABELS[key] ?? key.toUpperCase()}
											</th>
										))}
									</tr>
								</thead>
								<tbody>
									{profile.stats.map((row: any, i: number) => (
										<tr key={`${row.season}-${row.tid}-${i}`}>
											<td>{row.season}</td>
											<td>{row.abbrev}</td>
											<td className="text-end">{row.age}</td>
											{statKeys.map((key) => (
												<td key={key} className="text-end">
													{formatStat(row, key)}
												</td>
											))}
										</tr>
									))}
								</tbody>
								{profile.careerStats ? (
									<tfoot>
										<tr className="fw-bold">
											<td>Career</td>
											<td />
											<td />
											{statKeys.map((key) => (
												<td key={key} className="text-end">
													{formatStat(profile.careerStats, key)}
												</td>
											))}
										</tr>
									</tfoot>
								) : null}
							</table>
						</div>

						<a
							className="btn btn-sm btn-light-bordered mt-3"
							href={helpers.leagueUrl(["player", profile.pid])}
						>
							Full player page
						</a>
					</>
				)}
			</Modal.Body>
		</Modal>
	);
};
