import clsx from "clsx";
import {
	type ButtonHTMLAttributes,
	type MouseEvent,
	type ReactNode,
	type Ref,
} from "react";

export const ProcessingSpinner = ({
	text = "Processing",
}: {
	text?: string;
}) => {
	return (
		<>
			<span
				className="spinner-border spinner-border-sm"
				role="status"
				aria-hidden="true"
			></span>{" "}
			{text}
		</>
	);
};

export const styleGrid11 = {
	gridColumn: 1,
	gridRow: 1,
};

export const ActionButton = ({
	children,
	className,
	disabled,
	form,
	maintainWidth = true,
	onClick,
	processing,
	processingText,
	progress,
	ref,
	size,
	type,
	variant = "primary",
}: {
	children: ReactNode;
	className?: string;
	disabled?: boolean;
	maintainWidth?: boolean;
	onClick?: (event: MouseEvent<HTMLButtonElement>) => void;
	processing: boolean;
	processingText?: string;
	// While processing, a percent done: shown after the text and as a bar
	// along the button's bottom edge.
	progress?: number;
	ref?: Ref<HTMLButtonElement>;
	size?: "lg";
	variant?: "primary" | "secondary" | "god-mode" | "danger" | "light-bordered";
} & Pick<ButtonHTMLAttributes<HTMLButtonElement>, "form" | "type">) => {
	const showProgress = processing && progress !== undefined;
	const text = showProgress
		? `${processingText ?? "Processing"} ${progress}%`
		: processingText;

	// maintainWidth d-inline-grid stuff is from https://x.com/wesbos/status/1834242925401694490
	return (
		<button
			form={form}
			type={type}
			className={clsx(
				`btn btn-${variant}`,
				size ? `btn-${size}` : undefined,
				maintainWidth ? "d-inline-grid" : undefined,
				showProgress ? "position-relative overflow-hidden" : undefined,
				className,
			)}
			disabled={disabled || processing}
			onClick={onClick}
			ref={ref}
		>
			{maintainWidth ? (
				<>
					<span
						style={styleGrid11}
						className={processing ? "opacity-0" : "opacity-100"}
					>
						{children}
					</span>
					<span
						style={styleGrid11}
						className={processing ? "opacity-100" : "opacity-0"}
					>
						<ProcessingSpinner text={text} />
					</span>
				</>
			) : processing ? (
				<ProcessingSpinner text={text} />
			) : (
				children
			)}
			{showProgress ? (
				<span
					className="position-absolute bottom-0 start-0"
					style={{
						height: 4,
						width: `${progress}%`,
						backgroundColor: "currentColor",
						opacity: 0.85,
						transition: "width 0.2s",
					}}
				/>
			) : null}
		</button>
	);
};
