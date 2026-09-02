import { useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { toWorker } from "../util/toWorker.ts";
import type { View } from "../../common/types.ts";
import { Avatar, type TeamLike } from "./SocialPost.tsx";

// MANAGING FIVE HUNDRED ACCOUNTS.
//
// Two editing modes off one list, because the two jobs are genuinely
// different. Opening an account edits that account. Selecting several and
// applying a change edits all of them at once, which is the only practical way
// to shape a whole league: put every fan account on one archetype, quieten
// every player, delete the ones you do not want.
//
// The list is filtered rather than paged. Five hundred rows is too many to
// scroll and too few to justify pagination, and the thing people arrive
// wanting is one specific account they can name.

type Account = View<"socialAccounts">["accounts"][number];

const KINDS = [
	{ id: "all", label: "All" },
	{ id: "media", label: "Media & fans" },
	{ id: "team", label: "Teams" },
	{ id: "player", label: "Players" },
	{ id: "edited", label: "Edited" },
] as const;

const SocialAccounts = ({
	accounts,
	archetypes,
	errorMessage,
	handle,
	teams,
}: View<"socialAccounts">) => {
	useTitleBar({ title: "League Feed Accounts" });

	const [search, setSearch] = useState(handle ?? "");
	const [kind, setKind] = useState<(typeof KINDS)[number]["id"]>("all");
	const [selected, setSelected] = useState<Set<string>>(new Set());
	const [openId, setOpenId] = useState<string | undefined>(undefined);
	const [batchArchetype, setBatchArchetype] = useState("");
	const [status, setStatus] = useState("");

	if (errorMessage) {
		return <p className="alert alert-warning d-inline-block">{errorMessage}</p>;
	}

	const teamByTid = new Map<number, TeamLike>(
		teams.map((t: any) => [t.tid, t]),
	);

	const needle = search.trim().toLowerCase();
	const shown = accounts.filter((account: Account) => {
		if (kind === "edited" && account.implicit) {
			return false;
		}
		if (kind !== "all" && kind !== "edited" && account.kind !== kind) {
			return false;
		}
		if (needle === "") {
			return true;
		}
		return (
			account.name.toLowerCase().includes(needle) ||
			account.handle.toLowerCase().includes(needle)
		);
	});

	// Capped for the DOM's sake, not the user's: the filter is the way to find
	// something, and rendering five hundred editable rows helps nobody.
	const LIMIT = 120;
	const visible = shown.slice(0, LIMIT);

	const toggle = (id: string) => {
		setSelected((prev) => {
			const next = new Set(prev);
			if (next.has(id)) {
				next.delete(id);
			} else {
				next.add(id);
			}
			return next;
		});
	};

	const applyBatch = async (
		patch: Record<string, unknown>,
		description: string,
	) => {
		const ids = [...selected];
		if (ids.length === 0) {
			return;
		}
		await toWorker("main", "socialAccountsBatch", { ids, patch } as any);
		setStatus(
			`${description} for ${ids.length} ${helpers.plural("account", ids.length)}.`,
		);
		setSelected(new Set());
	};

	return (
		<>
			<p className="text-body-secondary">
				<a href={helpers.leagueUrl(["social"])}>Back to the feed</a> ·{" "}
				{accounts.length} accounts
			</p>

			<div className="d-flex flex-wrap gap-2 mb-3">
				<input
					className="form-control"
					onChange={(event) => setSearch(event.target.value)}
					placeholder="Search name or handle"
					style={{ maxWidth: 260 }}
					type="text"
					value={search}
				/>
				<select
					className="form-select"
					onChange={(event) => setKind(event.target.value as any)}
					style={{ maxWidth: 180 }}
					value={kind}
				>
					{KINDS.map((option) => (
						<option key={option.id} value={option.id}>
							{option.label}
						</option>
					))}
				</select>
				<NewAccountButton archetypes={archetypes} teams={teams} />
			</div>

			{selected.size > 0 ? (
				<div className="border rounded p-3 mb-3 bg-body-secondary bg-opacity-25">
					<div className="fw-bold mb-2">{selected.size} selected</div>
					<div className="d-flex flex-wrap gap-2 align-items-center">
						<select
							className="form-select"
							onChange={(event) => setBatchArchetype(event.target.value)}
							style={{ maxWidth: 220 }}
							value={batchArchetype}
						>
							<option value="">Set archetype…</option>
							{archetypes.map((a: any) => (
								<option key={a.id} value={a.id}>
									{a.label}
								</option>
							))}
						</select>
						<button
							className="btn btn-primary"
							disabled={batchArchetype === ""}
							onClick={() => {
								void applyBatch(
									{ archetypeId: batchArchetype },
									"Archetype set",
								);
							}}
							type="button"
						>
							Apply
						</button>
						<button
							className="btn btn-light-bordered"
							onClick={() => {
								void applyBatch(
									{ personality: { postiness: 0.05 } },
									"Quietened",
								);
							}}
							type="button"
						>
							Quieten
						</button>
						<button
							className="btn btn-danger ms-auto"
							onClick={() => {
								void applyBatch({ removed: true }, "Removed");
							}}
							type="button"
						>
							Remove
						</button>
						<button
							className="btn btn-light-bordered"
							onClick={() => setSelected(new Set())}
							type="button"
						>
							Clear
						</button>
					</div>
				</div>
			) : null}

			{status ? (
				<p className="alert alert-success d-inline-block py-1 px-2">{status}</p>
			) : null}

			{shown.length > LIMIT ? (
				<p className="text-body-secondary small">
					Showing {LIMIT} of {shown.length}. Narrow the search to see the rest.
				</p>
			) : null}

			<div className="list-group">
				{visible.map((account: Account) => (
					<div className="list-group-item" key={account.id}>
						<div className="d-flex align-items-center gap-3">
							<input
								checked={selected.has(account.id)}
								className="form-check-input mt-0 flex-shrink-0"
								onChange={() => toggle(account.id)}
								type="checkbox"
							/>
							<Avatar
								account={account as any}
								size={36}
								team={
									account.tid === undefined
										? undefined
										: teamByTid.get(account.tid)
								}
							/>
							<div className="flex-grow-1" style={{ minWidth: 0 }}>
								<div className="text-truncate">
									<a href={helpers.leagueUrl(["social", account.handle])}>
										{account.name}
									</a>{" "}
									<span className="text-body-secondary">@{account.handle}</span>
									{account.implicit ? null : (
										<span className="badge text-bg-light border ms-2">
											edited
										</span>
									)}
								</div>
								<div className="text-body-secondary small text-truncate">
									{account.archetypeId} · posts{" "}
									{Math.round(account.postiness * 100)}% of eligible days
								</div>
							</div>
							<button
								className="btn btn-light-bordered btn-sm flex-shrink-0"
								onClick={() =>
									setOpenId(openId === account.id ? undefined : account.id)
								}
								type="button"
							>
								{openId === account.id ? "Close" : "Edit"}
							</button>
						</div>

						{openId === account.id ? (
							<AccountEditor
								account={account}
								archetypes={archetypes}
								onDone={() => setOpenId(undefined)}
							/>
						) : null}
					</div>
				))}
			</div>

			{visible.length === 0 ? (
				<p className="text-body-secondary">No accounts match that.</p>
			) : null}
		</>
	);
};

const AccountEditor = ({
	account,
	archetypes,
	onDone,
}: {
	account: Account;
	archetypes: View<"socialAccounts">["archetypes"];
	onDone: () => void;
}) => {
	const [name, setName] = useState(account.name);
	const [handle, setHandle] = useState(account.handle);
	const [bio, setBio] = useState(account.bio);
	const [archetypeId, setArchetypeId] = useState(account.archetypeId);
	const [avatarUrl, setAvatarUrl] = useState(account.avatarUrl ?? "");
	const [coverUrl, setCoverUrl] = useState(account.coverUrl ?? "");
	const [saving, setSaving] = useState(false);

	const archetype = archetypes.find((a: any) => a.id === archetypeId);

	const save = async () => {
		setSaving(true);
		await toWorker("main", "socialAccountSave", {
			id: account.id,
			kind: account.kind,
			name,
			handle,
			bio,
			archetypeId,
			avatarUrl: avatarUrl === "" ? undefined : avatarUrl,
			coverUrl: coverUrl === "" ? undefined : coverUrl,
			pid: account.pid,
			tid: account.tid,
		} as any);
		setSaving(false);
		onDone();
	};

	return (
		<div className="mt-3 pt-3 border-top">
			<div className="row g-2">
				<div className="col-sm-6">
					<label className="form-label">Name</label>
					<input
						className="form-control"
						onChange={(event) => setName(event.target.value)}
						type="text"
						value={name}
					/>
				</div>
				<div className="col-sm-6">
					<label className="form-label">Handle</label>
					<input
						className="form-control"
						onChange={(event) => setHandle(event.target.value)}
						type="text"
						value={handle}
					/>
				</div>
				<div className="col-12">
					<label className="form-label">Bio</label>
					<input
						className="form-control"
						onChange={(event) => setBio(event.target.value)}
						type="text"
						value={bio}
					/>
				</div>
				<div className="col-sm-6">
					<label className="form-label">Personality</label>
					<select
						className="form-select"
						onChange={(event) => setArchetypeId(event.target.value)}
						value={archetypeId}
					>
						{archetypes.map((a: any) => (
							<option key={a.id} value={a.id}>
								{a.label}
							</option>
						))}
					</select>
					{archetype ? (
						<div className="form-text">{archetype.summary}</div>
					) : null}
				</div>
				<div className="col-sm-3">
					<label className="form-label">Picture URL</label>
					<input
						className="form-control"
						onChange={(event) => setAvatarUrl(event.target.value)}
						type="text"
						value={avatarUrl}
					/>
				</div>
				<div className="col-sm-3">
					<label className="form-label">Cover URL</label>
					<input
						className="form-control"
						onChange={(event) => setCoverUrl(event.target.value)}
						type="text"
						value={coverUrl}
					/>
				</div>
			</div>

			<div className="d-flex gap-2 mt-3">
				<button
					className="btn btn-primary"
					disabled={saving}
					onClick={() => void save()}
					type="button"
				>
					Save
				</button>
				{account.implicit ? null : (
					<button
						className="btn btn-light-bordered"
						onClick={async () => {
							await toWorker("main", "socialAccountReset", account.id as any);
							onDone();
						}}
						type="button"
					>
						Reset to default
					</button>
				)}
				<button
					className="btn btn-danger ms-auto"
					onClick={async () => {
						await toWorker("main", "socialAccountRemove", {
							id: account.id,
							kind: account.kind,
						} as any);
						onDone();
					}}
					type="button"
				>
					Remove
				</button>
			</div>
		</div>
	);
};

const NewAccountButton = ({
	archetypes,
	teams,
}: {
	archetypes: View<"socialAccounts">["archetypes"];
	teams: View<"socialAccounts">["teams"];
}) => {
	const [open, setOpen] = useState(false);
	const [name, setName] = useState("");
	const [archetypeId, setArchetypeId] = useState("beatWriter");
	const [tid, setTid] = useState("");

	if (!open) {
		return (
			<button
				className="btn btn-light-bordered"
				onClick={() => setOpen(true)}
				type="button"
			>
				Add account
			</button>
		);
	}

	return (
		<div className="d-flex flex-wrap gap-2 align-items-center">
			<input
				className="form-control"
				onChange={(event) => setName(event.target.value)}
				placeholder="Account name"
				style={{ maxWidth: 200 }}
				type="text"
				value={name}
			/>
			<select
				className="form-select"
				onChange={(event) => setArchetypeId(event.target.value)}
				style={{ maxWidth: 190 }}
				value={archetypeId}
			>
				{archetypes.map((a: any) => (
					<option key={a.id} value={a.id}>
						{a.label}
					</option>
				))}
			</select>
			<select
				className="form-select"
				onChange={(event) => setTid(event.target.value)}
				style={{ maxWidth: 190 }}
				value={tid}
			>
				<option value="">No team</option>
				{teams
					.filter((t: any) => !t.disabled)
					.map((t: any) => (
						<option key={t.tid} value={String(t.tid)}>
							{t.region} {t.name}
						</option>
					))}
			</select>
			<button
				className="btn btn-primary"
				disabled={name.trim() === ""}
				onClick={async () => {
					await toWorker("main", "socialAccountCreate", {
						name: name.trim(),
						archetypeId,
						tid: tid === "" ? undefined : Number.parseInt(tid),
					} as any);
					setName("");
					setOpen(false);
				}}
				type="button"
			>
				Create
			</button>
			<button
				className="btn btn-light-bordered"
				onClick={() => setOpen(false)}
				type="button"
			>
				Cancel
			</button>
		</div>
	);
};

export default SocialAccounts;
