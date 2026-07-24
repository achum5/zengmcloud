import type { View } from "../../common/types.ts";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { LiveGame } from "./LiveGame/index.tsx";

const IntrasquadGame = ({ liveSim, abbrev }: View<"intrasquadGame">) => {
	const teamName = (t: (typeof liveSim)["initialBoxScore"]["teams"][number]) =>
		`${t.region} ${t.name}`;
	useTitleBar({
		title: "Intrasquad Scrimmage",
		titleLong: `Intrasquad Scrimmage » ${teamName(
			liveSim.initialBoxScore.teams[0],
		)} vs ${teamName(liveSim.initialBoxScore.teams[1])}`,
		hideNewWindow: true,
	});

	return (
		<>
			<p>
				<a
					href={helpers.leagueUrl(
						abbrev !== undefined ? ["intrasquad", abbrev] : ["roster"],
					)}
				>
					Set up another scrimmage
				</a>
			</p>
			<LiveGame {...liveSim} />
		</>
	);
};

export default IntrasquadGame;
