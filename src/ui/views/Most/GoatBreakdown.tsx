import { useState } from "react";
import clsx from "clsx";
import { Modal } from "../../components/Modal.tsx";
import { toWorker } from "../../util/toWorker.ts";
import { helpers } from "../../util/helpers.ts";
import type api from "../../../worker/api/index.ts";

type Breakdown = Awaited<ReturnType<typeof api.main.getGoatBreakdown>>;
type Item = NonNullable<Breakdown>["items"][number];

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

// "3x Champion", or "PER 24.1" for something measured rather than counted
const describe = (item: Item) => {
	if (item.amount === undefined) {
		return item.label;
	}

	if (item.count) {
		return `${helpers.numberWithCommas(item.amount, 0)}x ${item.label}`;
	}

	return `${item.label}: ${helpers.numberWithCommas(item.amount, 1)}`;
};

const Rows = ({ items }: { items: Item[] }) => {
	const largest = Math.max(...items.map((item) => Math.abs(item.value)), 0);

	return (
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
				{items.map((item, i) => (
					<tr key={i}>
						<td
							title={item.formula}
							style={{ whiteSpace: "normal", overflowWrap: "anywhere" }}
						>
							{describe(item)}
						</td>
						<td
							className={clsx(
								"text-end align-middle",
								item.value < 0 ? "text-danger" : undefined,
							)}
							style={{ whiteSpace: "nowrap" }}
						>
							{formatGoatValue(item.value)}
						</td>
						<td className="align-middle">
							{largest > 0 ? (
								<div
									className={item.value < 0 ? "bg-danger" : "bg-primary"}
									style={{
										height: 6,
										borderRadius: 3,
										width: `${(Math.abs(item.value) / largest) * 100}%`,
									}}
								/>
							) : null}
						</td>
					</tr>
				))}
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

	const contributing =
		breakdown?.items.filter((item) => item.value !== 0) ?? [];
	const rest = breakdown?.items.filter((item) => item.value === 0) ?? [];

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
							<Rows items={contributing} />
							{rest.length > 0 ? (
								<details className="mt-3">
									<summary>No contribution ({rest.length})</summary>
									<ul className="list-unstyled small text-body-secondary mt-2 mb-0">
										{rest.map((item, i) => (
											<li key={i}>{item.label}</li>
										))}
									</ul>
								</details>
							) : null}
						</>
					)}
				</Modal.Body>
			</Modal>
		</>
	);
};
