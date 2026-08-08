import { OverlayTrigger, Popover } from "react-bootstrap";
import type { ContractValueBreakdown } from "../../common/contractValue.ts";
import { WINS_PER_VORP } from "../../common/contractValue.ts";
import { helpers } from "./helpers.ts";

// One Contract Value cell, shared by the three tables that show it.
//
// Signed and coloured because the sign IS the reading: the number is what the
// team saved (or wasted) against what the production was worth, so "+$8.1M" and
// "-$8.1M" are opposite verdicts on an identically sized contract, and a bare
// "8.1" would hide which one you were looking at.
//
// Clicking it opens the arithmetic. This number comes out of several steps -
// production to wins, wins to dollars, minus the salary - and a number nobody
// can check is a number nobody trusts, so every step is on screen with its
// inputs rather than being summarised as "advanced stats".

const money = (millions: number, precision = 1) =>
	helpers.formatCurrency(millions, "M", precision);

const Row = ({
	label,
	detail,
	value,
	strong,
	className,
}: {
	label: string;
	detail?: string;
	value: string;
	strong?: boolean;
	className?: string;
}) => (
	<div className={`d-flex justify-content-between gap-3 ${className ?? ""}`}>
		<div>
			<div className={strong ? "fw-bold" : undefined}>{label}</div>
			{detail ? (
				<div className="text-body-secondary small">{detail}</div>
			) : null}
		</div>
		<div className={`text-nowrap ${strong ? "fw-bold" : ""}`}>{value}</div>
	</div>
);

const Breakdown = ({ v }: { v: ContractValueBreakdown }) => {
	const positive = v.surplus > 0;
	return (
		<>
			<Row
				label="Production"
				detail={`${v.vorp.toFixed(1)} VORP × ${WINS_PER_VORP}`}
				value={`${v.war.toFixed(1)} wins`}
			/>
			<Row
				className="mt-2"
				label="Price of a win"
				detail="Measured from this league's payroll"
				value={money(v.dollarsPerWin, 2)}
			/>
			<hr className="my-2" />
			<Row
				label="Worth"
				detail={`${money(v.minContract, 2)} minimum + ${v.war.toFixed(1)} × ${money(v.dollarsPerWin, 2)}`}
				value={money(v.marketValue)}
			/>
			<Row className="mt-2" label="Paid" value={`−${money(v.salary)}`} />
			<hr className="my-2" />
			<Row
				label="Value"
				strong
				value={`${positive ? "+" : ""}${money(v.surplus)}`}
				className={positive ? "text-success" : "text-danger"}
			/>
			<div className="text-body-secondary small mt-2">
				{positive
					? "Produced more than the contract cost."
					: "Cost more than the production was worth."}
			</div>
		</>
	);
};

export const contractValueCell = (
	value: ContractValueBreakdown | undefined,
) => {
	if (value === undefined) {
		return null;
	}

	// Rounded before comparing, so a contract that displays as $0.0M isn't
	// coloured as though it were a bargain.
	const rounded = Math.round(value.surplus * 10) / 10;
	const formatted = `${rounded > 0 ? "+" : ""}${money(rounded)}`;

	return {
		value: (
			<OverlayTrigger
				trigger="click"
				placement="auto"
				rootClose
				overlay={
					<Popover>
						<Popover.Header as="h3">Contract Value</Popover.Header>
						<Popover.Body style={{ minWidth: 260 }}>
							<Breakdown v={value} />
						</Popover.Body>
					</Popover>
				}
			>
				<button
					type="button"
					// Looks like the plain number it replaces - the tables are dense
					// enough without a button chrome on every row.
					className={`btn btn-link p-0 border-0 align-baseline text-decoration-none ${
						rounded > 0
							? "text-success"
							: rounded < 0
								? "text-danger"
								: "text-body"
					}`}
					title="How this was worked out"
				>
					{formatted}
				</button>
			</OverlayTrigger>
		),
		// The raw number, so sorting is by actual value rather than by the
		// formatted string (where "-$9.0M" would outrank "+$10.0M").
		sortValue: value.surplus,
		searchValue: formatted,
	};
};
