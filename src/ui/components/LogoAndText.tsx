import { memo } from "react";
import { GAME_NAME } from "../../common/constants.ts";
import LogoSpinner from "./LogoSpinner.tsx";
import { useLocal } from "../util/local.ts";
import clsx from "clsx";
import { DEFAULT_LEAGUE_LOGO } from "./NewInterface/leagueLogo.ts";

type Props = {
	gold?: boolean;
	inLeague?: boolean;
	updating: boolean;
};
const LogoAndText = memo(({ gold, inLeague, updating }: Props) => {
	// workerBusy covers game sims, phase changes, the draft, auto play, etc.
	const { leagueLogo, newInterface, workerBusy } = useLocal([
		"leagueLogo",
		"newInterface",
		"workerBusy",
	]);
	const spinning = updating || workerBusy;

	// The new interface shows the league's own logo here, and the spinning ball
	// only while the game is working.
	const showLeagueLogo = inLeague && newInterface && !spinning;

	return (
		<a
			className={
				inLeague
					? "navbar-brand text-body-secondary d-none d-md-inline ms-md-2 ms-lg-0"
					: "navbar-brand text-body-secondary"
			}
			href="/"
		>
			{showLeagueLogo ? (
				<img
					className="ni-brand-logo"
					src={leagueLogo || DEFAULT_LEAGUE_LOGO}
					alt=""
					width={28}
					height={28}
				/>
			) : (
				<LogoSpinner gold={gold} size={18} spinning={spinning} />
			)}

			<span
				className={clsx(
					"logo-text",
					inLeague ? "d-none d-lg-inline" : undefined,
				)}
			>
				{GAME_NAME}
			</span>
		</a>
	);
});

export default LogoAndText;
