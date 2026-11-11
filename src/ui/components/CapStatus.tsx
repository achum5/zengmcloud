import { helpers } from "../util/helpers.ts";
import { ABBREVS, CAP_STATUS, optionAbbrev } from "../../common/signingHow.ts";
import type { CapStatusKey } from "../../common/signingHow.ts";
import type {
	CapException,
	TeamCapStatus,
} from "../../worker/core/realisticFrontOffice/capSheet.ts";

// A TEAM'S CAP STATUS under realistic front offices' contract rules, in plain
// words: where its payroll stands, the room it has under the cap and the hard
// cap, and the exception it has left. One component, on every page that deals
// in signing players.

const money = (amount: number) => helpers.formatCurrency(amount / 1000, "M");

// "Under the cap", "Over the tax"... as text, red only at the hard cap, where
// it stops a team signing anyone but minimum players.
// `inline`: after a payroll, "(under the cap)".
export const CapStatusText = ({
	className,
	inline,
	status,
}: {
	className?: string;
	inline?: boolean;
	status: CapStatusKey;
}) => {
	const label = CAP_STATUS[status].label;
	return (
		<span
			className={
				status === "hardCap"
					? `text-danger${className ? ` ${className}` : ""}`
					: className
			}
		>
			{inline ? `(${label.toLowerCase()})` : label}
		</span>
	);
};

export const exceptionText = (exception: CapException) =>
	exception.kind === "midLevel" ? (
		<>
			<abbr title={ABBREVS.MLE}>MLE</abbr> {money(exception.amount)}
		</>
	) : exception.kind === "used" ? (
		"Used"
	) : (
		"None"
	);

export const roomText = (amount: number) => (
	<span className={amount < 0 ? "text-danger" : undefined}>
		{Number.isFinite(amount) ? money(amount) : "-"}
	</span>
);

export const CapStatus = ({
	cap,
	className,
}: {
	cap: TeamCapStatus;
	className?: string;
}) => (
	<p className={className}>
		{cap.status === "hardCap" ? (
			<>
				<CapStatusText status={cap.status} />.{" "}
			</>
		) : null}
		{cap.capSpace >= 0 ? (
			<>
				Cap space: <b>{money(cap.capSpace)}</b> after cap holds.
			</>
		) : (
			<>
				Over the cap by <b>{money(-cap.capSpace)}</b>
				{cap.status === "apron" ? ", and over the luxury tax" : null}.
			</>
		)}{" "}
		{Number.isFinite(cap.hardCapRoom) ? (
			<>
				Room under the hard cap:{" "}
				<b className={cap.hardCapRoom < 0 ? "text-danger" : undefined}>
					{money(cap.hardCapRoom)}
				</b>
				.{" "}
			</>
		) : null}
		{cap.exception.kind === "midLevel" ? (
			<>
				Mid-level exception: <b>{money(cap.exception.amount)}</b>.{" "}
			</>
		) : cap.exception.kind === "used" ? (
			<>Mid-level exception: used. </>
		) : null}
		<a href={helpers.leagueUrl(["contract_rules"])}>Contract rules</a>
	</p>
);

// A contract's terms: its option, a prove-it deal. Abbreviations in the
// table's text, spelled out on hover.
export const termsText = (how: {
	option?: "player" | "team";
	proveIt?: true;
}) => {
	const parts = [];
	if (how.option) {
		const abbrev = optionAbbrev(how.option);
		parts.push(
			<abbr key="option" title={ABBREVS[abbrev]}>
				{abbrev}
			</abbr>,
		);
	}
	if (how.proveIt) {
		parts.push(
			<abbr key="proveIt" title={ABBREVS["Prove-it"]}>
				prove-it
			</abbr>,
		);
	}
	if (parts.length === 0) {
		return null;
	}
	return (
		<>
			{parts.map((part, i) => (
				<span key={i}>
					{i > 0 ? ", " : null}
					{part}
				</span>
			))}
		</>
	);
};
