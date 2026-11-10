import clsx from "clsx";
import { helpers } from "../util/helpers.ts";
import { toWorker } from "../util/toWorker.ts";
import { showNotification } from "../util/showNotification.ts";
import { ABBREVS, CAP_STATUS, optionAbbrev } from "../../common/signingHow.ts";
import type { CapStatusKey } from "../../common/signingHow.ts";
import type {
	CapException,
	TeamCapStatus,
} from "../../worker/core/realisticFrontOffice/capSheet.ts";

// A TEAM'S CAP STATUS under realistic front offices' contract rules: the
// badge, the room it has under each line, the exception it has left, and its
// cap holds. One component, on every page that deals in signing players.

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
	) : exception.kind === "taxpayerMidLevel" ? (
		<>
			<abbr title={ABBREVS.TMLE}>TMLE</abbr> {money(exception.amount)}
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

const renounce = async (pid: number) => {
	const error = await toWorker("main", "renounceFreeAgent", pid);
	if (error) {
		showNotification({ text: error, type: "error" } as any);
	}
};

export const CapHolds = ({ cap }: { cap: TeamCapStatus }) => {
	if (cap.holds.length === 0) {
		return null;
	}
	return (
		<table className="table table-sm table-nonfluid mb-0">
			<thead>
				<tr>
					<th title="His ask counts against your cap room until he signs or you renounce him">
						Cap holds
					</th>
					<th className="text-end">{money(cap.holdsTotal)}</th>
					{cap.canRenounce ? <th /> : null}
				</tr>
			</thead>
			<tbody>
				{cap.holds.map((row) => (
					<tr key={row.pid}>
						<td>
							<a href={helpers.leagueUrl(["player", row.pid])}>{row.name}</a>
							{row.restricted ? (
								<span
									className="badge text-bg-light border ms-1"
									title={ABBREVS.RFA}
								>
									RFA
								</span>
							) : null}
						</td>
						<td className="text-end">{money(row.amount)}</td>
						{cap.canRenounce ? (
							<td>
								<button
									className="btn btn-xs btn-light-bordered"
									onClick={() => renounce(row.pid)}
									title="Give up his Bird rights and cap hold"
								>
									Renounce
								</button>
							</td>
						) : null}
					</tr>
				))}
			</tbody>
		</table>
	);
};

export const CapStatus = ({
	cap,
	className,
	holds = true,
}: {
	cap: TeamCapStatus;
	className?: string;
	// Show the cap holds table too.
	holds?: boolean;
}) => {
	const items = [
		{
			label: cap.holdsTotal > 0 ? "Cap space after holds" : "Cap space",
			value: roomText(cap.capSpace),
		},
		{ label: "Room under apron", value: roomText(cap.apronRoom) },
		{ label: "Room under hard cap", value: roomText(cap.hardCapRoom) },
		{ label: "Exception", value: exceptionText(cap.exception) },
	];
	return (
		<div className={className}>
			<div className="d-flex flex-wrap align-items-end gap-3">
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
			{holds && cap.holds.length > 0 ? (
				<div className="mt-2">
					<CapHolds cap={cap} />
				</div>
			) : null}
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
