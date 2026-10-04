import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import type { View } from "../../common/types.ts";

type Props = Extract<View<"bracketology">, { college: true }>;
type TeamRow = Props["firstFourOut"][number];

const TeamLink = ({
	t,
	season,
	userTid,
}: {
	t: TeamRow;
	season: number;
	userTid: number;
}) => (
	<a
		href={helpers.leagueUrl(["roster", `${t.abbrev}_${t.tid}`, season])}
		className={t.tid === userTid ? "fw-bold" : undefined}
		title={`${t.region} ${t.name}, ${t.won}-${t.lost}`}
	>
		{t.region}
	</a>
);

const Bracketology = (props: View<"bracketology">) => {
	useTitleBar({ title: "Bracketology" });

	if (!props.college) {
		return <p>Bracketology is only in college leagues.</p>;
	}
	if (props.seedLines.length === 0) {
		return <p>Check back once the season starts.</p>;
	}

	const { season, userTid } = props;
	const autobids = new Set(props.autobids);

	return (
		<>
			<p>
				{props.projected ? (
					"Projected field if the season ended today."
				) : (
					<a href={helpers.leagueUrl(["playoffs", season])}>
						NCAA tournament bracket
					</a>
				)}
			</p>
			<table className="table table-sm table-striped w-auto">
				<thead>
					<tr>
						<th>Seed</th>
						<th colSpan={4}>Teams</th>
					</tr>
				</thead>
				<tbody>
					{props.seedLines.map((line) => (
						<tr key={line.seed}>
							<td>{line.seed}</td>
							{line.teams.map((t) => (
								<td key={t.tid} className="text-nowrap">
									<TeamLink t={t} season={season} userTid={userTid} />{" "}
									<span className="text-body-secondary small">
										{t.won}-{t.lost}
										{props.projected && autobids.has(t.tid)
											? ` · ${t.conf}`
											: ""}
									</span>
								</td>
							))}
						</tr>
					))}
				</tbody>
			</table>

			{props.projected ? (
				<div className="d-flex flex-wrap gap-5">
					<div>
						<h3>Last four in</h3>
						{props.lastFourIn.map((t) => (
							<div key={t.tid}>
								<TeamLink t={t} season={season} userTid={userTid} />{" "}
								<span className="text-body-secondary small">
									{t.won}-{t.lost}
								</span>
							</div>
						))}
					</div>
					<div>
						<h3>First four out</h3>
						{props.firstFourOut.map((t) => (
							<div key={t.tid}>
								<TeamLink t={t} season={season} userTid={userTid} />{" "}
								<span className="text-body-secondary small">
									{t.won}-{t.lost}
								</span>
							</div>
						))}
					</div>
				</div>
			) : null}

			{props.confTourneys.length > 0 ? (
				<>
					<h2 className="mt-4">Conference tournaments</h2>
					<table className="table table-sm table-striped w-auto">
						<tbody>
							{props.confTourneys.map((conf) => (
								<tr key={conf.cid}>
									<td>{conf.name}</td>
									<td>
										{conf.champ ? (
											<>
												<TeamLink
													t={conf.champ}
													season={season}
													userTid={userTid}
												/>{" "}
												<span className="text-body-secondary small">
													champion
												</span>
											</>
										) : (
											conf.alive.map((t, i) => (
												<span key={t.tid}>
													{i > 0 ? ", " : ""}
													<TeamLink t={t} season={season} userTid={userTid} />
												</span>
											))
										)}
									</td>
								</tr>
							))}
						</tbody>
					</table>
				</>
			) : null}

			{props.nit ? (
				<>
					<h2 className="mt-4">NIT</h2>
					{props.nit.champ !== undefined ? (
						<p>
							Champion:{" "}
							{(() => {
								const champ = props.nit.field.find(
									(t) => t.tid === props.nit!.champ,
								);
								return champ ? (
									<TeamLink t={champ} season={season} userTid={userTid} />
								) : null;
							})()}
						</p>
					) : null}
					<div className="d-flex flex-wrap gap-3">
						{props.nit.field.map((t, i) => (
							<span
								key={t.tid}
								className={
									props.nit!.alive.includes(t.tid)
										? undefined
										: "text-body-tertiary text-decoration-line-through"
								}
							>
								{i + 1}. <TeamLink t={t} season={season} userTid={userTid} />
							</span>
						))}
					</div>
				</>
			) : null}
		</>
	);
};

export default Bracketology;
