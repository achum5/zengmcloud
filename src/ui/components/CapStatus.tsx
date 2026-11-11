import clsx from "clsx";
import { helpers } from "../util/helpers.ts";
import { ABBREVS, CAP_STATUS, optionAbbrev } from "../../common/signingHow.ts";
import type { CapStatusKey } from "../../common/signingHow.ts";
import type {
	CapException,
	TeamCapStatus,
} from "../../worker/core/realisticFrontOffice/capSheet.ts";

// A TEAM'S CAP STATUS under realistic front offices' contract rules: the
// badge, the room it has under the cap and the hard cap, and the exception it
// has left. One component, on every page that deals in signing players.

const money = (amount: number) => helpers.formatCurrency(amount / 1000, "M");

export const CapStatusBadge = ({
	className,
	status,
}: {
	className?: string;
	status: CapStatusKey;
}) => (
	<span className={clsx("badge", CAP_STATUS[status].className, className)}>
		{CAP_STATUS[status].label}
	</span>
);

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
}) => {
	const items = [
		{ label: "Cap space", value: roomText(cap.capSpace) },
		{ label: "Room under hard cap", value: roomText(cap.hardCapRoom) },
		{ label: "Exception", value: exceptionText(cap.exception) },
	];
	return (
		<div className={clsx("d-flex flex-wrap align-items-end gap-3", className)}>
			<div>
				<CapStatusBadge className="fs-6" status={cap.status} />
			</div>
			{items.map((item) => (
				<div key={item.label}>
					<div className="text-body-secondary small">{item.label}</div>
					<div className="fw-bold">{item.value}</div>
				</div>
			))}
			<a href={helpers.leagueUrl(["contract_rules"])}>Contract Rules</a>
		</div>
	);
};

// A contract's terms: its option, a prove-it deal.
export const termsBadges = (how: {
	option?: "player" | "team";
	proveIt?: true;
}) => (
	<>
		{how.option ? (
			<span
				className="badge text-bg-light border me-1"
				title={ABBREVS[optionAbbrev(how.option)]}
			>
				{optionAbbrev(how.option)}
			</span>
		) : null}
		{how.proveIt ? (
			<span className="badge text-bg-warning me-1" title={ABBREVS["Prove-it"]}>
				Prove-it
			</span>
		) : null}
	</>
);
