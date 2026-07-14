import { useState } from "react";
import { MoreLinks } from "../components/MoreLinks.tsx";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { toWorker } from "../util/toWorker.ts";
import type { View } from "../../common/types.ts";
import { SafeHtml } from "../components/SafeHtml.tsx";

const Transactions = ({
	abbrev,
	eventType,
	events,
	season,
	tid,
}: View<"transactions">) => {
	useTitleBar({
		title: "Transactions",
		dropdownView: "transactions",
		dropdownFields: {
			teamsAndAll: abbrev,
			seasonsAndAll: season,
			eventType,
		},
	});

	const [copied, setCopied] = useState(false);
	const [fallback, setFallback] = useState<string | undefined>();

	// Dump every trade over the last 5 seasons (with records, trajectories, and
	// realized win shares) so the CPU trade AI can be reviewed.
	const copyTrades = async () => {
		setFallback(undefined);
		let dump: string;
		try {
			dump = await toWorker("main", "getTradeHistoryDump", 5);
		} catch (error) {
			// Surface the failure instead of silently doing nothing.
			setFallback(`Failed to build trade dump: ${(error as Error).message}`);
			return;
		}
		try {
			await navigator.clipboard.writeText(dump);
			setCopied(true);
			globalThis.setTimeout(() => setCopied(false), 3000);
		} catch {
			// Clipboard blocked — show the text to select manually.
			setFallback(dump);
		}
	};

	const moreLinks =
		abbrev !== "all" ? (
			<MoreLinks
				type="team"
				page="depth"
				abbrev={abbrev}
				tid={tid}
				season={season !== "all" ? season : undefined}
			/>
		) : (
			<p>
				More: <a href={helpers.leagueUrl(["news", "all", season])}>News Feed</a>
			</p>
		);

	return (
		<>
			{moreLinks}

			<div className="mb-3">
				<button
					className={`btn btn-sm ${copied ? "btn-success" : "btn-light-bordered"}`}
					onClick={copyTrades}
					title="Copy every trade from the last 5 seasons, with full detail"
				>
					{copied ? "✓ Copied" : "Copy trades (5 seasons)"}
				</button>
			</div>

			{fallback !== undefined ? (
				<textarea
					className="form-control mb-3"
					style={{ fontFamily: "monospace", fontSize: 11 }}
					rows={12}
					readOnly
					value={fallback}
					onFocus={(event) => event.target.select()}
				/>
			) : null}

			<ul className="list-group">
				{events.map((e) => (
					<li key={e.eid} className="list-group-item">
						<SafeHtml dirty={e.text} />
					</li>
				))}
			</ul>
		</>
	);
};

export default Transactions;
