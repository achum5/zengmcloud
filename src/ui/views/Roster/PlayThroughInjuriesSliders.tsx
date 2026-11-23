import { AnimatePresence, m } from "framer-motion";
import { useEffect, useState } from "react";
import { Dropdown } from "react-bootstrap";
import { timeBetweenGames } from "../../../common/timeBetweenGames.ts";
import playThroughInjuriesFactor from "../../../common/playThroughInjuriesFactor.ts";
import { HelpPopover } from "../../components/HelpPopover.tsx";
import CollapseArrow from "../../components/CollapseArrow.tsx";
import { helpers } from "../../util/helpers.ts";
import { toWorker } from "../../util/toWorker.ts";
import { useLocal } from "../../util/local.ts";

const Slider = ({
	className,
	initialValue,
	playoffs,
	days,
	tid,
}: {
	className?: string;
	initialValue: number;
	playoffs?: boolean;
	// On the NBA calendar: for each setting, the most days left a player can
	// show and still play.
	days?: number[];
	tid: number;
}) => {
	const [value, setValue] = useState(initialValue);

	// Reflect external changes to this setting (e.g. a teammate changing it in a
	// synced multiplayer league) - local state alone would ignore the new prop.
	useEffect(() => {
		setValue(initialValue);
	}, [initialValue]);

	const id = `play-through-injury-${playoffs ? "playoffs" : "regular-season"}`;

	const percent = 100 * playThroughInjuriesFactor(value);

	const rounded = Number.isInteger(percent)
		? String(percent)
		: percent.toFixed(1);

	return (
		<div className={className}>
			<label className="form-label mb-0" htmlFor={id}>
				{playoffs ? "Playoffs" : "Regular Season"}
			</label>
			<input
				type="range"
				className="form-range"
				id={id}
				value={value}
				min="0"
				max="10"
				step="1"
				onChange={async (event) => {
					const parsed = Number.parseInt(event.target.value);
					if (!Number.isNaN(parsed)) {
						setValue(parsed);
						await toWorker("main", "updatePlayThroughInjuries", {
							tid,
							value: parsed,
							playoffs,
						});
					}
				}}
			/>
			<div style={{ marginTop: -5 }}>
				{value === 0 ? (
					"Only play fully healthy players"
				) : (
					<>
						{days?.[value] === undefined ? (
							<>
								{value} {timeBetweenGames(value)}
							</>
						) : (
							<>
								{days[value]} {helpers.plural("day", days[value])}
							</>
						)}{" "}
						<span className="text-body-secondary">
							({rounded}% performance)
						</span>
					</>
				)}
			</div>
		</div>
	);
};

const titleText = "Play Through Injuries";

const InjuriesHelp = ({ gender }: { gender: "male" | "female" }) => (
	<HelpPopover className="ms-1" title={titleText}>
		<p>
			This allows you to determine when players should play through minor
			injuries. You can set the cutoff separately for the regular season and
			playoffs.
		</p>
		<p>
			The more games remaining on a player's injury, the worse{" "}
			{helpers.pronoun(gender, "he")} will play. So if a player comes back at
			90% performance and plays every day until {helpers.pronoun(gender, "he")}
			's healthy, {helpers.pronoun(gender, "he")} will gradually improve each
			game until {helpers.pronoun(gender, "he")} reaches 100%.
		</p>
		<p>
			Additionally, the injury rate is 50% higher when playing through an
			injury, which includes the possibility of a player either reaggravating{" "}
			{helpers.pronoun(gender, "his")} current injury or getting a new injury.
		</p>
	</HelpPopover>
);

type InjuriesTeam = {
	tid: number;
	playThroughInjuries: [number, number];
	// On the NBA calendar (regular season, playoffs; see Slider's days).
	playThroughDays?: [number[], number[]];
};

// THE REFRESHED LOOK'S: a small button by the roster's other buttons, its
// sliders in a panel that drops down over the page (nothing below it moves,
// and the page doesn't scroll) - open until a click outside it.
export const PlayThroughInjuriesMenu = ({ t }: { t: InjuriesTeam }) => {
	const { gender } = useLocal(["gender"]);
	const [show, setShow] = useState(false);
	return (
		<Dropdown
			show={show}
			autoClose="outside"
			onToggle={(next, meta) => {
				// (A tap in its help, which shows outside the panel, leaves it
				// open.)
				const target = meta.originalEvent?.target;
				if (
					!next &&
					meta.source === "rootClose" &&
					target instanceof Element &&
					target.closest(".popover")
				) {
					return;
				}
				setShow(next);
			}}
			className="pti-menu"
		>
			<Dropdown.Toggle
				variant="light-bordered"
				size="sm"
				id={`pti-menu-${t.tid}`}
				title={titleText}
			>
				<span className="d-md-none">Injuries</span>
				<span className="d-none d-md-inline">{titleText}</span>
			</Dropdown.Toggle>
			<Dropdown.Menu className="pti-menu-panel">
				<div className="d-flex align-items-center mb-2">
					<b>{titleText}</b>
					<InjuriesHelp gender={gender} />
				</div>
				<Slider
					className="mb-3"
					initialValue={t.playThroughInjuries[0]}
					days={t.playThroughDays?.[0]}
					tid={t.tid}
				/>
				<Slider
					playoffs
					initialValue={t.playThroughInjuries[1]}
					days={t.playThroughDays?.[1]}
					tid={t.tid}
				/>
			</Dropdown.Menu>
		</Dropdown>
	);
};

const PlayThroughInjuriesSliders = ({ t }: { t: InjuriesTeam }) => {
	const folds = window.mobile;
	const [expanded, setExpanded] = useState(!folds);

	const { gender } = useLocal(["gender"]);

	return (
		<div className="play-through-injuries">
			<div className="d-flex align-items-center">
				{folds ? (
					<button
						className="btn btn-link p-0 fw-bold"
						type="button"
						onClick={() => setExpanded((prev) => !prev)}
					>
						<CollapseArrow open={expanded} /> {titleText}
					</button>
				) : (
					<b>{titleText}</b>
				)}
				{expanded ? <InjuriesHelp gender={gender} /> : null}
			</div>
			<AnimatePresence initial={false}>
				{expanded ? (
					<m.form
						className="mt-2"
						initial="collapsed"
						animate="open"
						exit="collapsed"
						variants={{
							open: { opacity: 1, height: "auto" },
							collapsed: { opacity: 0, height: 0 },
						}}
						transition={{
							duration: 0.3,
							type: "tween",
						}}
					>
						<Slider
							className="mb-3"
							initialValue={t.playThroughInjuries[0]}
							days={t.playThroughDays?.[0]}
							tid={t.tid}
						/>
						<Slider
							playoffs
							initialValue={t.playThroughInjuries[1]}
							days={t.playThroughDays?.[1]}
							tid={t.tid}
						/>
					</m.form>
				) : null}
			</AnimatePresence>
		</div>
	);
};

export default PlayThroughInjuriesSliders;
