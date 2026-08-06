# Change-log retention

The multiplayer change log (`leagues/{code}/changes`) used to be append-only
forever. Nothing deleted a changeset, so a league simmed for months carried
every multi-MB sim day it had ever published — data every device consumed long
ago. That is a Firestore storage bill that only goes up.

Three pieces fix it. **The first two are already in the code; the third and
fourth are console actions you have to take.**

---

## 1. Entries carry a `ttlAt` (shipped)

Every change published from this build stamps `ttlAt = now + 3 days`
(`RETENTION_DAYS` in `src/common/syncRetention.ts`). The field is inert on its
own — readers ignore it completely — so it is safe to ship well ahead of the
policy below.

The log is a delivery buffer, not an archive: every device already holds the
same league file and applies deltas as they arrive, so an entry is dead weight
once everyone has read it. **The cost of a window this short is that a device
away longer than it, while the others played, must re-import a fresh export.**
Raise `RETENTION_DAYS` if that starts happening to anyone.

Changing the number only affects entries published *after* the change — older
ones keep the `ttlAt` they were stamped with. To clear existing history now,
use **Trim** in the Multiplayer Sync page's admin section.

### v2 rooms

v2 deltas live in `leagues/{code}/control` and carry the same `ttlAt`. They used
to be pruned when a checkpoint superseded them; nothing builds checkpoints any
more (see `AUTO_PUBLISH_CHECKPOINTS`), so the TTL is what bounds them. A policy
on the `control` collection group is safe: Firestore TTL only deletes documents
that *have* the field, so the state pointer, live broadcast and chat docs
sharing that collection are untouched.

## 2. A device that is too far behind is stopped, not silently broken (shipped)

This is the part that makes deleting history safe at all.

Catch-up is a `ts >` range read against the device's watermark. Deleting
entries a device has **already applied** is harmless — that's the whole point.
But a device away longer than the retention window needs entries that no longer
exist, and a range read finds *nothing missing*: it would quietly declare itself
current while holding stale records. That is the same silent-divergence failure
as the duplicated-games incident.

So on connect, the device reads the seq of the **oldest entry still in the log**
and compares it to its own watermark (`isTooFarBehind`). If the oldest survivor
is newer than the watermark, the entries in between are gone, and the connect
fails with a message telling the user to get a fresh export and re-import.

It is deliberately based on the log's real contents rather than on
`Date.now() - RETENTION_MS`, so a device with a wrong clock cannot lock itself
out for no reason, or — much worse — wave through a real gap. A failed probe
falls through rather than locking out, so a flaky read never becomes a lockout.

## 3. Turn on the TTL policy (you must do this)

The `ttlAt` field does nothing until Firestore is told to act on it.

**Console:** Firestore Database → **Time-to-live** tab → *Create policy* →
collection group `changes`, timestamp field `ttlAt`. Repeat for the `control`
collection group to cover v2 deltas.

The same tab tells you whether a policy already exists — if none is listed,
nothing has ever been deleted and the log holds the room's entire history
regardless of what `RETENTION_DAYS` says.

**Or gcloud:**

```
gcloud firestore fields ttls update ttlAt \
  --collection-group=changes \
  --enable-ttl \
  --project=zengmcloud-4a454

gcloud firestore fields ttls update ttlAt \
  --collection-group=control \
  --enable-ttl \
  --project=zengmcloud-4a454
```

Deletion is asynchronous and best-effort — expect entries to disappear within
about 24 hours of expiry, not on the minute. TTL deletes are billed as ordinary
document deletes, which is comfortably inside the free tier at this volume
(current usage is 0 deletes/day against a 20K/day allowance).

## 4. Trim the history that predates `ttlAt` (you must do this once)

**This is the step that actually reclaims the storage you are paying for.**
Every entry written before this build has no `ttlAt`, so the TTL policy will
never touch it — and that backlog *is* the bill.

Multiplayer Sync page → *Manage rooms* → unlock → set the day count →
**Trim history in all rooms**. It sweeps every room, deleting change docs older
than the cutoff, in adaptively-sized batches (change docs are big enough to hit
Firestore's ~10 MiB commit ceiling long before its 500-write one).

Run it once now. After that the TTL policy keeps up on its own.

⚠️ Anyone who has not synced since the cutoff will be locked out by the check in
§2 and will need a fresh export. That is the intended trade and the reason the
check exists — the alternative is not "they're fine", it's "they diverge
silently".

---

## Also worth doing: index exemptions

There is no `firestore.indexes.json` in this repo, so Firestore auto-indexes
**every** field — including the `changeset` and `payloadPart` strings, which run
to ~300 KB and are never queried by value. Index entries count toward billable
storage.

Exempt them: Firestore Database → **Indexes** → *Single field* → *Add exemption*
→ collection group `changes`, field `changeset` (then `payloadPart`) → disable
ascending, descending, and array-contains.

Nothing queries either field, so there is no query to break.

## Changing the window

`RETENTION_DAYS` in `src/common/syncRetention.ts` is the single source of
truth: it stamps `ttlAt`, defaults the admin trim box, and appears in the
too-far-behind message. Changing it only affects entries published afterward —
the TTL policy uses the stamp already written on each document.
