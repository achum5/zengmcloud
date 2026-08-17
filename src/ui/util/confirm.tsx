import {
	useCallback,
	useEffect,
	useRef,
	useState,
	type ReactNode,
} from "react";
import { confirmable, createConfirmation } from "react-confirm";
import { Modal } from "../components/Modal.tsx";

const Confirm = confirmable<
	{
		confirmation: ReactNode;
		defaultValue?: string;
		okText?: string;
		cancelText?: string;
		title?: string;
		danger?: boolean;
	},
	boolean | string | null
>(
	({
		show,
		proceed,
		confirmation,
		defaultValue,
		okText,
		cancelText,
		title,
		danger,
	}) => {
		okText = okText ?? "OK";
		cancelText = cancelText ?? "Cancel";
		const [controlledValue, setControlledValue] = useState(defaultValue ?? "");
		const ok = useCallback(
			() => proceed(defaultValue === undefined ? true : controlledValue),
			[controlledValue, defaultValue, proceed],
		);
		const cancel = useCallback(
			() => proceed(defaultValue === undefined ? false : null),
			[defaultValue, proceed],
		);
		const inputRef = useRef<HTMLInputElement>(null);
		const okRef = useRef<HTMLButtonElement>(null);
		const cancelRef = useRef<HTMLButtonElement>(null);

		// A danger confirm opens with CANCEL focused, so the reflex that got you
		// here - Enter, space, another click - backs out instead of committing. The
		// whole point of these is that the user was not really looking.
		useEffect(() => {
			if (inputRef.current) {
				inputRef.current.select();
			} else if (danger && cancelRef.current) {
				cancelRef.current.focus();
			} else if (okRef.current) {
				okRef.current.focus();
			}
		}, [danger]);

		return (
			<Modal
				show={show}
				onHide={cancel}
				className="highest-modal"
				backdropClassName="highest-modal-backdrop"
			>
				{title !== undefined ? (
					<Modal.Header className={danger ? "bg-danger text-white" : undefined}>
						<Modal.Title>{title}</Modal.Title>
					</Modal.Header>
				) : null}
				<Modal.Body>
					{confirmation}
					{defaultValue !== undefined ? (
						<form
							className="mt-3"
							onSubmit={(event) => {
								event.preventDefault();
								ok();
							}}
						>
							<input
								ref={inputRef}
								type="text"
								className="form-control"
								value={controlledValue}
								onChange={(event) => {
									setControlledValue(event.target.value);
								}}
							/>
						</form>
					) : null}
				</Modal.Body>

				<Modal.Footer>
					<button
						className="btn btn-secondary"
						onClick={cancel}
						ref={cancelRef}
					>
						{cancelText}
					</button>
					<button
						className={`btn btn-${danger ? "danger" : "primary"}`}
						onClick={ok}
						ref={okRef}
					>
						{okText}
					</button>
				</Modal.Footer>
			</Modal>
		);
	},
);

const confirmFunction = createConfirmation(Confirm);

export function confirm(
	message: ReactNode,
	options: {
		defaultValue: string;
		okText?: string;
		cancelText?: string;
		title?: string;
		danger?: boolean;
	},
): Promise<string | null>;

export function confirm(
	message: ReactNode,
	options?: {
		defaultValue?: undefined;
		okText?: string;
		cancelText?: string;
		title?: string;
		danger?: boolean;
	},
): Promise<boolean>;

export function confirm(
	message: ReactNode,
	{
		defaultValue,
		okText,
		cancelText,
		title,
		danger,
	}: {
		defaultValue?: string;
		okText?: string;
		cancelText?: string;
		// Shown in a modal header above the message. Without one the modal is just
		// the message and its buttons, as it always was.
		title?: string;
		// A red header and OK button, and Cancel focused instead of OK.
		danger?: boolean;
	} = {},
) {
	return confirmFunction({
		confirmation: message,
		defaultValue,
		okText,
		cancelText,
		title,
		danger,
	});
}
