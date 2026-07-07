import type { ReactNode } from "react";

// A deliberately small, SAFE Markdown renderer for game recaps. It renders React
// elements (never raw HTML / dangerouslySetInnerHTML), so pasted AI text can't
// inject markup. Covers the subset recaps use: headings, bold, italic, inline
// code, links, blockquotes, and unordered lists. Not a full CommonMark parser.

// Only allow safe link targets (http/https or in-app relative), else render the
// link as plain text.
const safeHref = (href: string): string | undefined => {
	const h = href.trim();
	if (/^https?:\/\//i.test(h) || h.startsWith("/") || h.startsWith("#")) {
		return h;
	}
	return undefined;
};

// Inline formatting within one line of text → React nodes.
const renderInline = (text: string, keyPrefix: string): ReactNode[] => {
	const patterns: {
		re: RegExp;
		node: (m: RegExpExecArray, key: string) => ReactNode;
	}[] = [
		// Bold/italic recurse so nested formatting AND links (e.g. an auto-linked
		// player name inside a **bold headline**) render, not as literal markup.
		{
			re: /\*\*([^*]+)\*\*/,
			node: (m, k) => <strong key={k}>{renderInline(m[1]!, k)}</strong>,
		},
		{
			re: /__([^_]+)__/,
			node: (m, k) => <strong key={k}>{renderInline(m[1]!, k)}</strong>,
		},
		{
			re: /\*([^*]+)\*/,
			node: (m, k) => <em key={k}>{renderInline(m[1]!, k)}</em>,
		},
		{
			re: /(?<![\dA-Za-z])_([^_]+)_(?![\dA-Za-z])/,
			node: (m, k) => <em key={k}>{renderInline(m[1]!, k)}</em>,
		},
		{ re: /`([^`]+)`/, node: (m, k) => <code key={k}>{m[1]}</code> },
		{
			re: /\[([^\]]+)]\(([^)]+)\)/,
			node: (m, k) => {
				const href = safeHref(m[2]!);
				return href ? (
					<a key={k} href={href}>
						{m[1]}
					</a>
				) : (
					m[1]
				);
			},
		},
	];

	const nodes: ReactNode[] = [];
	let remaining = text;
	let i = 0;
	while (remaining.length > 0) {
		let best: { index: number; len: number; node: ReactNode } | undefined;
		for (const { re, node } of patterns) {
			const m = re.exec(remaining);
			if (m && (best === undefined || m.index < best.index)) {
				best = {
					index: m.index,
					len: m[0].length,
					node: node(m, `${keyPrefix}-${i}`),
				};
			}
		}
		if (!best) {
			nodes.push(remaining);
			break;
		}
		if (best.index > 0) {
			nodes.push(remaining.slice(0, best.index));
		}
		nodes.push(best.node);
		remaining = remaining.slice(best.index + best.len);
		i += 1;
	}
	return nodes;
};

export const Markdown = ({ children }: { children: string }) => {
	const lines = children.replaceAll("\r\n", "\n").split("\n");
	const blocks: ReactNode[] = [];

	let paragraph: string[] = [];
	let list: string[] = [];

	const flushParagraph = () => {
		if (paragraph.length > 0) {
			const key = `p-${blocks.length}`;
			blocks.push(
				<p key={key} className="mb-2">
					{renderInline(paragraph.join(" "), key)}
				</p>,
			);
			paragraph = [];
		}
	};
	const flushList = () => {
		if (list.length > 0) {
			const key = `ul-${blocks.length}`;
			blocks.push(
				<ul key={key} className="mb-2">
					{list.map((item, idx) => (
						<li key={idx}>{renderInline(item, `${key}-${idx}`)}</li>
					))}
				</ul>,
			);
			list = [];
		}
	};

	for (const raw of lines) {
		const line = raw.trimEnd();
		const heading = /^(#{1,6})\s+(.*)$/.exec(line);
		const bullet = /^\s*[*-]\s+(.*)$/.exec(line);
		const quote = /^>\s?(.*)$/.exec(line);

		if (line.trim() === "") {
			flushParagraph();
			flushList();
		} else if (heading) {
			flushParagraph();
			flushList();
			const level = Math.min(6, Math.max(4, heading[1]!.length + 3));
			const Tag = `h${level}` as "h4" | "h5" | "h6";
			const key = `h-${blocks.length}`;
			blocks.push(
				<Tag key={key} className="mb-1">
					{renderInline(heading[2]!, key)}
				</Tag>,
			);
		} else if (bullet) {
			flushParagraph();
			list.push(bullet[1]!);
		} else if (quote) {
			flushParagraph();
			flushList();
			const key = `bq-${blocks.length}`;
			blocks.push(
				<blockquote
					key={key}
					className="border-start ps-2 text-body-secondary mb-2"
				>
					{renderInline(quote[1]!, key)}
				</blockquote>,
			);
		} else {
			flushList();
			paragraph.push(line.trim());
		}
	}
	flushParagraph();
	flushList();

	return <div className="markdown">{blocks}</div>;
};

export default Markdown;
