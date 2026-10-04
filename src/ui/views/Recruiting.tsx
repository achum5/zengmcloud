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
import { nilReaction } from "../../common/college.ts";
import type { View } from "../../common/types.ts";
import type { RecruitAction } from "../../worker/core/college/recruiting.ts";

// The recruiting board. Spend your weekly hours, offer scholarships with NIL
// money, and bring players in on official visits. Players commit when a
// school with an offer pulls clearly ahead; signing day makes it official.

type Recruit = Extract<
	View<"recruiting">,
	{ college: true }
>["recruits"][number];

const STAR_COLORS = ["", "#adb5bd", "#adb5bd", "#0d6efd", "#fd7e14", "#dc3545"];

const Stars = ({ stars }: { stars: number }) => (
	<span style={{ color: STAR_COLORS[stars], whiteSpace: "nowrap" }}>
		{"★".repeat(stars)}
		<span className="text-body-tertiary">{"★".repeat(5 - stars)}</span>
	</span>
);

const InterestBar = ({ value }: { value: number }) => {
	// Interest runs past 100 for a player a school has worked hard on.
	const pct = helpers.bound(value / 1.25, 0, 100);
	const color =
		value >= 70 ? "bg-success" : value >= 50 ? "bg-warning" : "bg-secondary";
	return (
		<div className="d-flex align-items-center gap-1" style={{ minWidth: 90 }}>
			<div className="progress flex-grow-1" style={{ height: 6 }}>
				<div className={`progress-bar ${color}`} style={{ width: `${pct}%` }} />
			</div>
			<span className="small text-body-secondary" style={{ width: 22 }}>
				{Math.round(value)}
			</span>
		</div>
	);
};

const fmtNil = (amount: number) => helpers.formatCurrency(amount / 1000, "M");

const act = async (action: RecruitAction) => {
	const error = await toWorker("main", "collegeRecruitAction", action);
	if (error) {
		showNotification({ type: "error", text: error });
	}
};

const REACTION_TEXT = {
	thrilled: { text: "Thrilled", className: "text-success" },
	happy: { text: "Happy", className: "text-success" },
	lukewarm: { text: "Lukewarm", className: "text-warning" },
	insulted: { text: "Insulted", className: "text-danger" },
};

const OfferModal = ({
	recruit,
	nilRoom,
	onHide,
}: {
	recruit: Recruit | undefined;
	nilRoom: number;
	onHide: () => void;
}) => {
	const [amount, setAmount] = useState<string>("");
	const [prevPid, setPrevPid] = useState<number | undefined>();
	if (recruit && recruit.pid !== prevPid) {
		setPrevPid(recruit.pid);
		setAmount(String(recruit.offer ?? recruit.ask));
	}
	if (!recruit) {
		return null;
	}

	const value = Number(amount);
	const valid = Number.isFinite(value) && value >= 0;
	const reaction = REACTION_TEXT[nilReaction(recruit, valid ? value : 0)];
	const room =
		nilRoom + (recruit.committed !== undefined ? (recruit.offer ?? 0) : 0);

	return (
		<Modal show onHide={onHide}>
			<Modal.Header closeButton>
				{recruit.firstName} {recruit.lastName} <Stars stars={recruit.stars} />
			</Modal.Header>
			<Modal.Body>
				<p>
					Asking <b>{fmtNil(recruit.ask)}</b>/yr · <b>{fmtNil(room)}</b> left in
					budget
				</p>
				<label className="form-label" htmlFor="recruit-nil">
					NIL offer (thousands per year)
				</label>
				<div className="input-group mb-2">
					<span className="input-group-text">$</span>
					<input
						id="recruit-nil"
						type="number"
						className="form-control"
						min={0}
						step={5}
						value={amount}
						onChange={(event) => setAmount(event.target.value)}
					/>
					<span className="input-group-text">k</span>
				</div>
				<div className={reaction.className}>{reaction.text}</div>
			</Modal.Body>
			<Modal.Footer>
				{recruit.offer !== undefined ? (
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
				) : null}
				<button type="button" className="btn btn-secondary" onClick={onHide}>
					Cancel
				</button>
				<button
					type="button"
					className="btn btn-primary"
					disabled={!valid}
					onClick={async () => {
						await act({ type: "offer", pid: recruit.pid, nil: value });
						onHide();
					}}
				>
					{recruit.offer !== undefined ? "Update offer" : "Offer scholarship"}
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

	if (!props.college) {
		return <p>Recruiting is only in college leagues.</p>;
	}

	const { recruits, team, userTid } = props;
	const nilRoom = team.nilBudget - team.nilCommitted;
	const commits = recruits.filter((r) => r.committed === userTid);
	const shown = onlyBoard
		? recruits.filter(
				(r) => r.hours > 0 || r.offer !== undefined || r.committed === userTid,
			)
		: recruits;

	const cols: Col[] = [
		{ title: "#", desc: "National Rank", sortType: "number" },
		{ title: "Stars", sortSequence: ["desc", "asc"], sortType: "number" },
		{ title: "Name", sortType: "name" },
		{ title: "Pos" },
		{ title: "Ht", sortType: "number" },
		{ title: "Home", desc: "Hometown" },
		{ title: "Ovr", sortSequence: ["desc", "asc"], sortType: "number" },
		{ title: "Pot", sortSequence: ["desc", "asc"], sortType: "number" },
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

	const rows: DataTableRow[] = shown.map((r) => ({
		key: r.pid,
		metadata: {
			type: "player",
			pid: r.pid,
			season: props.season,
			playoffs: "regularSeason",
		},
		classNames: { "table-info": r.committed === userTid },
		data: [
			r.rank,
			{
				value: <Stars stars={r.stars} />,
				sortValue: r.stars,
				searchValue: `${r.stars}`,
			},
			wrappedPlayerNameLabels({
				pid: r.pid,
				firstName: r.firstName,
				lastName: r.lastName,
				skills: r.ratings.skills,
			}),
			r.ratings.pos,
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
			r.ratings.ovr,
			r.ratings.pot,
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
						className={`btn btn-xs ${r.offer !== undefined ? "btn-success" : "btn-light-bordered"}`}
						onClick={() => setOfferPid(r.pid)}
					>
						{r.offer !== undefined ? fmtNil(r.offer) : "Offer"}
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
			<OfferModal
				recruit={recruits.find((r) => r.pid === offerPid)}
				nilRoom={nilRoom}
				onHide={() => setOfferPid(undefined)}
			/>

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

			<div className="form-check mb-2">
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

			<DataTable
				cols={cols}
				defaultSort={[0, "asc"]}
				defaultStickyCols={window.mobile ? 1 : 3}
				name="Recruiting"
				pagination
				rows={rows}
			/>
		</>
	);
};

export default Recruiting;
