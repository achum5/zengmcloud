import { useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { showNotification } from "../util/showNotification.ts";
import { DataTable } from "../components/DataTable/index.tsx";
import type { Col, DataTableRow } from "../components/DataTable/index.tsx";
import { wrappedPlayerNameLabels } from "../components/PlayerNameLabels.tsx";
import { Modal } from "../components/Modal.tsx";
import { fmtNil, NilNegotiation } from "../components/CollegeNil.tsx";
import { COLLEGE_PRIORITY_LABELS } from "../../common/college.ts";
import type { View } from "../../common/types.ts";
import type { RetentionAction } from "../../worker/core/college/retention.ts";

// After the season: renegotiate NIL with returning players, and try to keep
// the ones thinking about the transfer portal.

type Row = Extract<View<"retention">, { college: true }>["players"][number];

const act = async (action: RetentionAction) => {
	const result = await toWorker("main", "collegeRetentionAction", action);
	if (result.error) {
		showNotification({ type: "error", text: result.error });
	}
	return result.outcome;
};

const Risk = ({ risk, reasons }: { risk: number; reasons: string[] }) => {
	const [text, className] =
		risk >= 0.35
			? ["Considering it", "text-danger fw-bold"]
			: risk >= 0.15
				? ["Medium", "text-warning"]
				: ["Low", "text-success"];
	return (
		<span className={className} title={reasons.join(", ")}>
			{text}
		</span>
	);
};

const NegotiateModal = ({
	row,
	room,
	onHide,
}: {
	row: Row;
	room: number;
	onHide: () => void;
}) => (
	<Modal show onHide={onHide}>
		<Modal.Header closeButton>
			{row.firstName} {row.lastName}
		</Modal.Header>
		<Modal.Body>
			<NilNegotiation
				range={row.demandRange}
				room={room + row.nil}
				current={row.nil}
				counter={row.counter}
				patience={row.patience}
				walked={row.walked}
				onOffer={(nil) => act({ type: "nil", pid: row.pid, nil })}
			/>
		</Modal.Body>
		<Modal.Footer>
			<button type="button" className="btn btn-secondary" onClick={onHide}>
				Close
			</button>
		</Modal.Footer>
	</Modal>
);

const Retention = (props: View<"retention">) => {
	useTitleBar({ title: "Retention" });
	const [pid, setPid] = useState<number | undefined>();

	if (!props.college) {
		return <p>Retention is only in college leagues.</p>;
	}

	const { players, open } = props;
	const room = props.nilBudget - props.nilCommitted;

	if (!open || players.length === 0) {
		return <p>Opens after the season.</p>;
	}

	const cols: Col[] = [
		{ title: "Name", sortType: "name" },
		{ title: "Pos" },
		{ title: "Class", desc: "Next season" },
		{ title: "Ovr", sortSequence: ["desc", "asc"], sortType: "number" },
		{ title: "Pot", sortSequence: ["desc", "asc"], sortType: "number" },
		{ title: "MPG", sortSequence: ["desc", "asc"], sortType: "number" },
		{ title: "Wants", desc: "His top priorities", noSearch: true },
		{ title: "NIL", sortSequence: ["desc", "asc"], sortType: "number" },
		{ title: "Asking", desc: "NIL he wants next season", sortType: "number" },
		{ title: "Portal", desc: "Portal risk", sortType: "number" },
		{ title: "Promise", desc: "Playing time promise for next season" },
	];

	const rows: DataTableRow<"player">[] = players.map((r) => ({
		key: r.pid,
		metadata: {
			type: "player",
			pid: r.pid,
			season: props.season,
			playoffs: "regularSeason",
		},
		data: [
			wrappedPlayerNameLabels({
				pid: r.pid,
				firstName: r.firstName,
				lastName: r.lastName,
				skills: r.skills,
			}),
			r.pos,
			r.classLabel,
			r.ovr,
			r.pot,
			r.mpg.toFixed(1),
			{
				value: (
					<span className="small text-nowrap">
						{r.priorities.map((key) => COLLEGE_PRIORITY_LABELS[key]).join(", ")}
					</span>
				),
				sortValue: r.priorities.join(","),
			},
			{ value: fmtNil(r.nil), sortValue: r.nil },
			{
				value: r.settled ? (
					<span className="text-success">Settled</span>
				) : r.walked ? (
					<span className="text-danger">No deal</span>
				) : (
					<button
						type="button"
						className="btn btn-xs btn-light-bordered"
						onClick={() => setPid(r.pid)}
					>
						{fmtNil(r.demandRange[0])}–{fmtNil(r.demandRange[1])}
					</button>
				),
				sortValue: r.settled ? 0 : r.demandRange[1],
			},
			{
				value: <Risk risk={r.risk} reasons={r.reasons} />,
				sortValue: r.risk,
			},
			r.promise ? (
				<span className="text-success">
					{r.promise.type === "starter" ? "Starter" : `${r.promise.value}+ mpg`}
				</span>
			) : (
				<span className="d-flex gap-1">
					<button
						type="button"
						className="btn btn-xs btn-light-bordered"
						onClick={() =>
							void act({
								type: "promise",
								pid: r.pid,
								promise: "minutes",
								value: 20,
							})
						}
					>
						20+ mpg
					</button>
					<button
						type="button"
						className="btn btn-xs btn-light-bordered"
						onClick={() =>
							void act({ type: "promise", pid: r.pid, promise: "starter" })
						}
					>
						Starter
					</button>
				</span>
			),
		],
	}));

	const row = players.find((r) => r.pid === pid);

	return (
		<>
			{row ? (
				<NegotiateModal
					key={row.pid}
					row={row}
					room={room}
					onHide={() => setPid(undefined)}
				/>
			) : null}
			<p>
				NIL left: <b>{fmtNil(room)}</b> of {fmtNil(props.nilBudget)}
			</p>
			<DataTable
				cols={cols}
				defaultSort={[9, "desc"]}
				defaultStickyCols={1}
				name="Retention"
				rows={rows}
			/>
		</>
	);
};

export default Retention;
