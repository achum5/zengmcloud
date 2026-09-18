import { useState } from "react";
import clsx from "clsx";
import { Modal } from "../../components/Modal.tsx";
import { toWorker } from "../../util/toWorker.ts";
import { helpers } from "../../util/helpers.ts";
import type api from "../../../worker/api/index.ts";

type Breakdown = Awaited<ReturnType<typeof api.main.getGoatBreakdown>>;

export const formatGoatValue = (rawValue: number) => {
	// A negated zero term would otherwise print as "-0"
	const value = Object.is(rawValue, -0) ? 0 : rawValue;

	if (Math.abs(value) < 1_000_000) {
		const numDigits = Number.parseInt(Math.abs(value).toString()).toString()
			.length;

		// Show 3 decimal places if it's 1 digit integer part, and decrease by 1 as the integer length increases
		const maximumFractionDigits = Math.max(4 - numDigits, 0);
		return helpers.numberWithCommas(value, maximumFractionDigits);
	}

	return value.toPrecision(3);
};

const Rows = ({ rows }: { rows: NonNullable<Breakdown>["rows"] }) => {
	// Bars are scaled against the largest top-level term, so a term's share of
	// the score is readable at a glance. Nested terms aren't barred - they're
	// already inside a bar above them.
	const largest = Math.max(
		...rows
			.filter((row) => row.depth === 0 && row.value !== undefined)
			.map((row) => Math.abs(row.value!)),
		0,
	);

	return (
		// Fixed layout with the expression clipped to one line. Formula terms run
		// long, and left to itself the monospace text widens the table until the
		// values - the whole point of the panel - are pushed off the side.
		<table
			className="table table-sm mb-0"
			style={{ tableLayout: "fixed", width: "100%" }}
		>
			<colgroup>
				<col />
				<col style={{ width: 84 }} />
				<col style={{ width: 52 }} />
			</colgroup>
			<tbody>
				{rows.map((row, i) => {
					// A term that is broken down below only needs to be recognizable -
					// its children spell it out - so it gets one clipped line. A leaf is
					// short and is the actual content, so it wraps instead of being cut
					// off, which matters at phone width where there is no hover to
					// recover the rest.
					const summarized = (rows[i + 1]?.depth ?? -1) > row.depth;

					return (
						<tr key={i}>
							<td
								className="font-monospace small"
								title={row.text}
								style={{
									paddingLeft: 8 + row.depth * 16,
									...(summarized
										? {
												whiteSpace: "nowrap" as const,
												overflow: "hidden",
												textOverflow: "ellipsis",
												maxWidth: 0,
											}
										: {
												// The app's table CSS sets nowrap, so a leaf needs
												// this to actually wrap instead of running off the row
												whiteSpace: "normal" as const,
												overflowWrap: "anywhere" as const,
											}),
								}}
							>
								{row.text}
							</td>
							<td
								className={clsx(
									"text-end align-middle",
									row.depth === 0 ? "fw-bold" : "text-body-secondary",
									row.value !== undefined && row.value < 0
										? "text-danger"
										: undefined,
								)}
								style={{ whiteSpace: "nowrap" }}
							>
								{row.value === undefined ? "?" : formatGoatValue(row.value)}
							</td>
							<td className="align-middle">
								{row.depth === 0 && row.value !== undefined && largest > 0 ? (
									<div
										className={row.value < 0 ? "bg-danger" : "bg-primary"}
										style={{
											height: 6,
											borderRadius: 3,
											width: `${(Math.abs(row.value) / largest) * 100}%`,
										}}
									/>
								) : null}
							</td>
						</tr>
					);
				})}
			</tbody>
		</table>
	);
};

export const GoatBreakdown = ({
	name,
	pid,
	season,
	value,
}: {
	name: string;
	pid: number;
	season?: number;
	value: number;
}) => {
	const [show, setShow] = useState(false);

	// undefined means "not fetched yet". A fetch that comes back with nothing is
	// a real answer - the player has no stats the formula can read - so it needs
	// to be distinguishable from still loading, or the modal spins forever.
	const [state, setState] = useState<
		{ status: "loaded"; breakdown: Breakdown } | { status: "failed" }
	>();

	const open = async () => {
		setShow(true);

		if (state === undefined) {
			try {
				const breakdown = await toWorker("main", "getGoatBreakdown", {
					pid,
					season,
				});
				setState({ status: "loaded", breakdown });
			} catch {
				setState({ status: "failed" });
			}
		}
	};

	const breakdown = state?.status === "loaded" ? state.breakdown : undefined;

	return (
		<>
			<button
				className="btn btn-link p-0 border-0 align-baseline"
				onClick={open}
				title="Breakdown"
			>
				{formatGoatValue(value)}
			</button>
			<Modal show={show} onHide={() => setShow(false)} size="lg" scrollable>
				<Modal.Header closeButton>
					<Modal.Title>
						{name}
						{season !== undefined ? ` ${season}` : null}
					</Modal.Title>
				</Modal.Header>
				<Modal.Body>
					{state === undefined ? (
						<p className="mb-0">Loading...</p>
					) : breakdown === undefined ? (
						<p className="mb-0">No stats for the formula to read.</p>
					) : (
						<>
							<div className="d-flex justify-content-between align-items-baseline mb-2">
								<span className="text-body-secondary">GOAT</span>
								<span className="fs-4 fw-bold">
									{formatGoatValue(breakdown.total)}
								</span>
							</div>
							<Rows rows={breakdown.rows} />
							<details className="mt-3">
								<summary>Variables</summary>
								<ul
									className="list-unstyled font-monospace small mt-2 mb-0"
									style={{ columnWidth: 180 }}
								>
									{breakdown.variables.map((variable) => (
										<li key={variable.name}>
											{variable.name} ={" "}
											{helpers.numberWithCommas(variable.value, 2)}
										</li>
									))}
								</ul>
							</details>
						</>
					)}
				</Modal.Body>
			</Modal>
		</>
	);
};
