# Change-log retention

The multiplayer change log (`leagues/{code}/changes`) used to be append-only
forever. Nothing deleted a changeset, so a league simmed for months carried
every multi-MB sim day it had ever published — data every device consumed long
ago. That is a Firestore storage bill that only goes up.

Three pieces fix it. **The first two are already in the code; the third and
fourth are console actions you have to take.**

---

## 1. Entries carry a `ttlAt` (shipped)

Every change published from this build stamps `ttlAt = now + 45 days`
(`src/common/syncRetention.ts`). The field is inert on its own — readers
ignore it completely — so it is safe to ship well ahead of the policy below.

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
collection group `changes`, timestamp field `ttlAt`.

**Or gcloud:**

```
gcloud firestore fields ttls update ttlAt \
  --collection-group=changes \
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
