import { useEffect, useRef, useState } from "react";
import type api from "../../../worker/api/index.ts";
import { Modal } from "../../components/Modal.tsx";
import {
	AmountStepper,
	amountForYears,
	formatChance,
	formatOfferAmount,
	ShapePickers,
	TermButtons,
	type OptionChoice,
} from "../../components/OfferEditor.tsx";
import { useLocal } from "../../util/local.ts";
import { showNotification } from "../../util/showNotification.ts";
import { toWorker } from "../../util/toWorker.ts";

// CBA-lite: an offer to extend a player in the last season of his contract,
// answered on the spot.

type ExtensionProps = Exclude<
	Awaited<ReturnType<typeof api.main.getExtensionProps>>,
	string
>;
type ExtensionResult = Exclude<
	Awaited<ReturnType<typeof api.main.offerExtension>>,
	string
>;

export type ExtendTarget = { pid: number; name: string };

const offerOf = (
	amount: number,
	years: number,
	raise: number,
	option: OptionChoice,
) => ({
	amount,
	years,
	raise: raise / 100,
	option: option === "" || years < 2 ? undefined : option,
});

const Editor = ({
	name,
	pid,
	props,
}: {
	name: string;
	pid: number;
	props: ExtensionProps;
}) => {
	const { gameSimInProgress } = useLocal(["gameSimInProgress"]);
	const [years, setYears] = useState(props.wantedYears);
	const [amount, setAmount] = useState(
		props.terms.find((term) => term.years === props.wantedYears)!.ask,
	);
	const [raise, setRaise] = useState(0);
	const [option, setOption] = useState<OptionChoice>("");
	const [live, setLive] = useState<{ chance?: number; error?: string }>({
		chance: props.chance,
		error: props.error,
	});
	const [result, setResult] = useState<ExtensionResult>();
	const declined = result?.declined ?? props.declined;
	const [saving, setSaving] = useState(false);
	const term = props.terms.find((term) => term.years === years)!;

	// His odds and the rules, asked of the worker as the offer changes.
	const request = useRef(0);
	const first = useRef(true);
	useEffect(() => {
		if (first.current) {
			first.current = false;
			return;
		}
		if (result?.accepted) {
			return;
		}
		const id = ++request.current;
		setLive((prev) => ({ ...prev, chance: undefined }));
		const timer = setTimeout(async () => {
			const next = await toWorker("main", "getExtensionProps", {
				pid,
				offer: offerOf(amount, years, raise, option),
			});
			if (id === request.current) {
				setLive(
					typeof next === "string"
						? { error: next }
						: { chance: next.chance, error: next.error },
				);
			}
		}, 150);
		return () => {
			clearTimeout(timer);
		};
	}, [pid, amount, years, raise, option, result]);

	const save = async () => {
		setSaving(true);
		try {
			const response = await toWorker("main", "offerExtension", {
				pid,
				...offerOf(amount, years, raise, option),
			});
			if (typeof response === "string") {
				showNotification({ type: "error", text: response });
			} else {
				setResult(response);
			}
		} finally {
			setSaving(false);
		}
	};

	return (
		<div style={{ maxWidth: 575 }}>
			<TermButtons
				terms={props.terms}
				years={years}
				wantedYears={props.wantedYears}
				onPick={(next) => {
					setAmount(amountForYears(props.terms, amount, years, next));
					setYears(next);
				}}
			/>

			<div className="d-flex flex-wrap align-items-center gap-3 mb-3">
				<AmountStepper
					amount={amount}
					ask={term.ask}
					disabled={result?.accepted}
					maxContract={props.maxContract}
					minContract={props.minContract}
					onChange={setAmount}
				/>
				<div>
					Asking for {formatOfferAmount(term.ask)} thru {term.exp}
				</div>
			</div>

			<ShapePickers
				maxRaise={Math.round(props.maxRaise * 1000) / 10}
				option={option}
				raise={raise}
				setOption={setOption}
				setRaise={setRaise}
				years={years}
			/>

			<div className="d-flex flex-wrap align-items-center gap-2">
				<div className="me-auto">
					Your odds:{" "}
					<b data-testid="extension-odds">{formatChance(live.chance)}</b>
				</div>
				{result?.accepted || declined > 0 ? (
					<span
						className={result?.accepted ? "text-success" : "text-danger"}
						data-testid="extension-result"
					>
						{result?.accepted
							? "Accepted"
							: `Declined (${declined} of ${props.patience})`}
					</span>
				) : null}
				{live.error && !result?.accepted ? (
					<span className="text-danger small">{live.error}</span>
				) : null}
				<button
					type="button"
					className="btn btn-success"
					disabled={
						saving ||
						gameSimInProgress ||
						live.error !== undefined ||
						result?.accepted
					}
					onClick={save}
					title={`Offer ${name} ${formatOfferAmount(amount)} a year thru ${term.exp}`}
				>
					Offer
				</button>
			</div>
		</div>
	);
};

export const ExtendModal = ({
	close,
	target,
}: {
	close: () => void;
	target: ExtendTarget | undefined;
}) => {
	const [props, setProps] = useState<ExtensionProps>();
	const pid = target?.pid;
	useEffect(() => {
		setProps(undefined);
		if (pid === undefined) {
			return;
		}
		let current = true;
		(async () => {
			const next = await toWorker("main", "getExtensionProps", { pid });
			if (!current) {
				return;
			}
			if (typeof next === "string") {
				showNotification({ type: "error", text: next });
				close();
			} else {
				setProps(next);
			}
		})();
		return () => {
			current = false;
		};
	}, [pid, close]);

	return (
		<Modal show={target !== undefined} onHide={close}>
			<Modal.Header closeButton>
				<Modal.Title>Extend {target?.name}</Modal.Title>
			</Modal.Header>
			<Modal.Body>
				{target && props ? (
					<Editor
						key={target.pid}
						name={target.name}
						pid={target.pid}
						props={props}
					/>
				) : (
					"Loading..."
				)}
			</Modal.Body>
		</Modal>
	);
};
