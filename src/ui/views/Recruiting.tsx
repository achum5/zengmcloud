import { useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { helpers } from "../util/helpers.ts";
import { showNotification } from "../util/showNotification.ts";
import { DataTable } from "../components/DataTable/index.tsx";
import type { Col, DataTableRow } from "../components/DataTable/index.tsx";
import { wrappedPlayerNameLabels } from "../components/PlayerNameLabels.tsx";
import { CountryFlag } from "../components/CountryFlag.tsx";
import { Modal } from "../components/Modal.tsx";
import { Height } from "../components/Height.tsx";
import {
	fmtNil,
	fmtRange,
	InterestBar,
	NilNegotiation,
	Stars,
} from "../components/CollegeNil.tsx";
import {
	COLLEGE_PRIORITY_LABELS,
	COLLEGE_PROMISE_LABELS,
	type CollegePromiseType,
} from "../../common/college.ts";
import type { View } from "../../common/types.ts";
import type { RecruitAction } from "../../worker/core/college/recruiting.ts";

// The recruiting board. Spend your weekly hours (which also scouts players),
// negotiate scholarship offers with NIL money and promises, and bring players
// in on official visits.

type Recruit = Extract<
	View<"recruiting">,
	{ college: true }
>["recruits"][number];

const act = async (action: RecruitAction) => {
	const result = await toWorker("main", "collegeRecruitAction", action);
	if (result.error) {
		showNotification({ type: "error", text: result.error });
	}
	return result.outcome;
};

const PROMISE_TYPES: CollegePromiseType[] = [
	"starter",
	"minutes",
	"nilRaise",
	"noPosition",
];

const OfferModal = ({
	recruit,
	nilRoom,
	onHide,
}: {
	recruit: Recruit;
	nilRoom: number;
	onHide: () => void;
}) => {
	const [promises, setPromises] = useState<Set<CollegePromiseType>>(
		() => new Set(recruit.promises.map((promise) => promise.type)),
	);
	const [minutes, setMinutes] = useState(
		recruit.promises.find((promise) => promise.type === "minutes")?.value ?? 20,
	);
	const promiseList = () =>
		[...promises].map((type) =>
			type === "minutes" ? { type, value: minutes } : { type },
		);
	const room =
		nilRoom + (recruit.committed !== undefined ? (recruit.offer ?? 0) : 0);

	return (
		<Modal show onHide={onHide}>
			<Modal.Header closeButton>
				{recruit.firstName} {recruit.lastName} <Stars stars={recruit.stars} />
			</Modal.Header>
			<Modal.Body>
				{recruit.priorities.length > 0 ? (
					<p className="text-body-secondary">
						{recruit.priorities
							.map((key) => COLLEGE_PRIORITY_LABELS[key])
							.join(" · ")}
					</p>
				) : null}
				<NilNegotiation
					key={recruit.pid}
					range={recruit.askRange}
					room={room}
					current={recruit.offer}
					counter={recruit.counter}
					patience={recruit.patience}
					walked={recruit.walked}
					onOffer={(nil) =>
						act({
							type: "offer",
							pid: recruit.pid,
							nil,
							promises: promiseList(),
						})
					}
				/>
				<div className="mt-3">
					<div className="fw-bold mb-1">Promises</div>
					{PROMISE_TYPES.map((type) => (
						<div className="form-check form-check-inline" key={type}>
							<input
								className="form-check-input"
								type="checkbox"
								id={`promise-${type}`}
								checked={promises.has(type)}
								onChange={(event) => {
									const next = new Set(promises);
									if (event.target.checked) {
										next.add(type);
									} else {
										next.delete(type);
									}
									setPromises(next);
								}}
							/>
							<label className="form-check-label" htmlFor={`promise-${type}`}>
								{COLLEGE_PROMISE_LABELS[type]}
							</label>
						</div>
					))}
					{promises.has("minutes") ? (
						<select
							className="form-select form-select-sm d-inline-block w-auto"
							value={minutes}
							onChange={(event) => setMinutes(Number(event.target.value))}
						>
							{[10, 15, 20, 25, 30].map((x) => (
								<option key={x} value={x}>
									{x}+ mpg
								</option>
							))}
						</select>
					) : null}
				</div>
			</Modal.Body>
			<Modal.Footer>
				{recruit.offer !== undefined ? (
					<>
						<button
							type="button"
							className="btn btn-danger me-auto"
							onClick={async () => {
								await act({ type: "pull", pid: recruit.pid });
								onHide();
							}}
						>
							Pull offer
						</button>
						<button
							type="button"
							className="btn btn-light-bordered"
							onClick={() =>
								void act({
									type: "offer",
									pid: recruit.pid,
									nil: recruit.offer!,
									promises: promiseList(),
								})
							}
						>
							Save promises
						</button>
					</>
				) : null}
				<button type="button" className="btn btn-secondary" onClick={onHide}>
					Close
				</button>
			</Modal.Footer>
		</Modal>
	);
};

const HoursInput = ({ recruit, max }: { recruit: Recruit; max: number }) => {
	const [value, setValue] = useState(String(recruit.hours));
	const [prev, setPrev] = useState(recruit.hours);
	if (recruit.hours !== prev) {
		setPrev(recruit.hours);
		setValue(String(recruit.hours));
	}
	const commit = () => {
		const hours = Number(value);
		if (Number.isFinite(hours) && hours !== recruit.hours) {
			void act({ type: "hours", pid: recruit.pid, hours });
		}
	};
	return (
		<input
			type="number"
			className="form-control form-control-sm"
			style={{ width: 64 }}
			min={0}
			max={max}
			step={5}
			value={value}
			onChange={(event) => setValue(event.target.value)}
			onBlur={commit}
			onKeyDown={(event) => {
				if (event.key === "Enter") {
					commit();
				}
			}}
		/>
	);
};

const Recruiting = (props: View<"recruiting">) => {
	useTitleBar({ title: "Recruiting" });
	const [offerPid, setOfferPid] = useState<number | undefined>();
	const [onlyBoard, setOnlyBoard] = useState(false);
	const [onlyPortal, setOnlyPortal] = useState(false);

	if (!props.college) {
		return <p>Recruiting is only in college leagues.</p>;
	}

	const { recruits, team, userTid } = props;
	const nilRoom = team.nilBudget - team.nilCommitted;
	const commits = recruits.filter((r) => r.committed === userTid);
	const shown = recruits.filter(
		(r) =>
			(!onlyBoard ||
				r.hours > 0 ||
				r.offer !== undefined ||
				r.committed === userTid) &&
			(!onlyPortal || r.portalFrom !== undefined),
	);
	const offerRecruit = recruits.find((r) => r.pid === offerPid);

	const cols: Col[] = [
		{ title: "#", desc: "National Rank", sortType: "number" },
		{ title: "Name", sortType: "name" },
		{ title: "Stars", sortSequence: ["desc", "asc"], sortType: "number" },
		{ title: "Pos" },
		{ title: "Ht", sortType: "number" },
		{ title: "Home", desc: "Hometown" },
		{
			title: "Ovr",
			desc: "Overall rating, as well as you've scouted him",
			sortSequence: ["desc", "asc"],
			sortType: "number",
		},
		{
			title: "Pot",
			desc: "Potential rating, as well as you've scouted him",
			sortSequence: ["desc", "asc"],
			sortType: "number",
		},
		{ title: "Wants", desc: "His top priorities", noSearch: true },
		{
			title: "Interest",
			desc: "Interest in your school",
			sortSequence: ["desc", "asc"],
			sortType: "number",
		},
		{ title: "Leaders", desc: "Schools leading for him", noSearch: true },
		{ title: "Status" },
		{
			title: "Hours",
			desc: "Hours per week",
			sortSequence: ["desc", "asc"],
			sortType: "number",
		},
		{
			title: "Offer",
			desc: "Your NIL offer",
			sortSequence: ["desc", "asc"],
			sortType: "number",
		},
		{ title: "Visit", desc: "Official visit" },
	];

	const rows: DataTableRow<"player">[] = shown.map((r) => ({
		key: r.pid,
		metadata: {
			type: "player",
			pid: r.pid,
			season: props.season,
			playoffs: "regularSeason",
		},
		classNames: { "table-info": r.committed === userTid },
		data: [
			{
				value: r.portalFrom !== undefined ? `P${r.rank}` : r.rank,
				// Transfers first while the portal is open.
				sortValue: r.portalFrom !== undefined ? r.rank - 100000 : r.rank,
			},
			wrappedPlayerNameLabels({
				pid: r.pid,
				firstName: r.firstName,
				lastName: r.lastName,
				skills: r.skills,
			}),
			{
				value: <Stars stars={r.stars} />,
				sortValue: r.stars,
				searchValue: `${r.stars}`,
			},
			r.pos,
			{ value: <Height inches={r.hgt} />, sortValue: r.hgt },
			{
				value: (
					<span className="d-flex align-items-center gap-1">
						<CountryFlag country={r.bornLoc} />
						{r.state ?? ""}
					</span>
				),
				sortValue: r.state ?? r.bornLoc,
				searchValue: r.bornLoc,
			},
			{
				value: <span title={`${r.scouted}% scouted`}>{fmtRange(r.ovr)}</span>,
				sortValue: (r.ovr[0] + r.ovr[1]) / 2,
			},
			{
				value: <span title={`${r.scouted}% scouted`}>{fmtRange(r.pot)}</span>,
				sortValue: (r.pot[0] + r.pot[1]) / 2,
			},
			{
				value: (
					<span className="small text-nowrap">
						{r.priorities.map((key) => COLLEGE_PRIORITY_LABELS[key]).join(", ")}
					</span>
				),
				sortValue: r.priorities.join(","),
			},
			{ value: <InterestBar value={r.interest} />, sortValue: r.interest },
			{
				value: (
					<span className="small text-nowrap">
						{r.top.map((t, i) => (
							<span
								key={t.tid}
								className={t.tid === userTid ? "fw-bold" : undefined}
							>
								{i > 0 ? ", " : ""}
								{t.abbrev} {Math.round(t.interest)}
							</span>
						))}
					</span>
				),
				sortValue: r.top[0]?.interest ?? 0,
			},
			r.committed !== undefined
				? {
						value: (
							<span
								className={
									r.committed === userTid ? "text-success fw-bold" : undefined
								}
							>
								{r.committedAbbrev}
							</span>
						),
						sortValue: r.committedAbbrev ?? "",
					}
				: r.portalFrom
					? `Portal (${r.portalFrom})`
					: "Open",
			{
				value: <HoursInput recruit={r} max={team.maxHoursPerRecruit} />,
				sortValue: r.hours,
			},
			{
				value: (
					<button
						type="button"
						className={`btn btn-xs ${r.offer !== undefined ? "btn-success" : r.walked ? "btn-outline-danger" : r.counter !== undefined ? "btn-warning" : "btn-light-bordered"}`}
						onClick={() => setOfferPid(r.pid)}
					>
						{r.offer !== undefined
							? fmtNil(r.offer)
							: r.walked
								? "Done"
								: r.counter !== undefined
									? "Talks"
									: "Offer"}
					</button>
				),
				sortValue: r.offer ?? -1,
			},
			r.visited ? (
				<span className="text-success">Visited</span>
			) : (
				<button
					type="button"
					className="btn btn-xs btn-light-bordered"
					disabled={r.offer === undefined || team.visitsUsed >= team.visitsMax}
					onClick={() => void act({ type: "visit", pid: r.pid })}
				>
					Invite
				</button>
			),
		],
	}));

	return (
		<>
			{offerRecruit ? (
				<OfferModal
					key={offerRecruit.pid}
					recruit={offerRecruit}
					nilRoom={nilRoom}
					onHide={() => setOfferPid(undefined)}
				/>
			) : null}

			<div className="d-flex flex-wrap gap-2 mb-3">
				<div className="trivia-tile">
					<div className="trivia-tile-value">
						{team.hoursUsed}/{team.hoursMax}
					</div>
					<div className="trivia-tile-label">Hours / week</div>
				</div>
				<div className="trivia-tile">
					<div className="trivia-tile-value">{team.open}</div>
					<div className="trivia-tile-label">Open scholarships</div>
				</div>
				<div className="trivia-tile">
					<div className="trivia-tile-value">{team.offers}</div>
					<div className="trivia-tile-label">Offers out</div>
				</div>
				<div className="trivia-tile">
					<div className="trivia-tile-value">
						{team.visitsUsed}/{team.visitsMax}
					</div>
					<div className="trivia-tile-label">Visits</div>
				</div>
				<div className="trivia-tile">
					<div className="trivia-tile-value">{fmtNil(nilRoom)}</div>
					<div className="trivia-tile-label">
						NIL left of {fmtNil(team.nilBudget)}
					</div>
				</div>
				<div className="trivia-tile" title="How well you keep promises">
					<div className="trivia-tile-value">{Math.round(team.rep * 100)}</div>
					<div className="trivia-tile-label">Promise rep</div>
				</div>
				<div className="form-check form-switch align-self-center ms-2">
					<input
						className="form-check-input"
						type="checkbox"
						id="auto-recruit"
						checked={team.auto}
						onChange={(event) =>
							void toWorker("main", "collegeSetAutoRecruit", {
								auto: event.target.checked,
							})
						}
					/>
					<label className="form-check-label" htmlFor="auto-recruit">
						Auto-recruit
					</label>
				</div>
			</div>

			{commits.length > 0 ? (
				<p>
					<b>Committed:</b>{" "}
					{commits.map((r, i) => (
						<span key={r.pid}>
							{i > 0 ? ", " : ""}
							<a href={helpers.leagueUrl(["player", r.pid])}>
								{r.firstName} {r.lastName}
							</a>{" "}
							<Stars stars={r.stars} />
						</span>
					))}
				</p>
			) : null}

			<div className="d-flex gap-3 mb-2">
				<div className="form-check">
					<input
						className="form-check-input"
						type="checkbox"
						id="only-board"
						checked={onlyBoard}
						onChange={(event) => setOnlyBoard(event.target.checked)}
					/>
					<label className="form-check-label" htmlFor="only-board">
						Only my board
					</label>
				</div>
				{props.portalOpen ? (
					<div className="form-check">
						<input
							className="form-check-input"
							type="checkbox"
							id="only-portal"
							checked={onlyPortal}
							onChange={(event) => setOnlyPortal(event.target.checked)}
						/>
						<label className="form-check-label" htmlFor="only-portal">
							Only portal
						</label>
					</div>
				) : null}
			</div>

			<DataTable
				cols={cols}
				defaultSort={[0, "asc"]}
				defaultStickyCols={2}
				name="Recruiting"
				pagination
				rows={rows}
			/>
		</>
	);
};

export default Recruiting;
