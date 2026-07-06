# Phone push notifications — setup

This adds real phone push notifications to ZenGM Cloud multiplayer: your
league-mates get a notification **even when ZenGM is completely closed** when

- the host finishes a sim,
- a trade or roster move happens, or
- the league reaches a phase that needs a human (draft, re-signing, …).

Everything except the one-time console setup below is already in the code.
There is no way to do "app fully closed" push without a server component, so
this uses a **Firebase Cloud Function** plus **Firebase Cloud Messaging (FCM)** —
the same Firebase project you already use for league sync.

---

## What the code already does

- **Client (UI thread):** `src/ui/util/pushNotifications.ts` requests permission
  and an FCM token, and registers it. The "Phone notifications" card on the
  **Tools → Multiplayer Sync** page drives it.
- **Client (worker):** after each action, `src/worker/core/sync/notifications.ts`
  decides if the change is notification-worthy and enqueues a doc at
  `leagues/{code}/notifications`. Device tokens live at `leagues/{code}/members/{uid}`.
- **Service worker:** `public/firebase-messaging-sw.js` shows the notification
  when the app is closed/backgrounded (dependency-free; no CDN).
- **Cloud Function:** `functions/index.js` fans each queued notification out to
  the room's devices (minus the author) via FCM.
- **Rules:** `firestore.rules` already allows the `members` and `notifications`
  collections (each device writes only its own token; the queue is append-only).

## What you have to do once (console + deploy)

### 1. Put the project on the Blaze plan

Cloud Functions require Blaze (pay-as-you-go). For a few users the usage is far
inside the free allowances, so the real cost is effectively **$0** — but a
billing account must be attached, and a budget cap is fine.

Firebase console → ⚙️ **Usage and billing** → **Details & settings** → **Modify plan** → Blaze.

### 2. Generate the Web Push (VAPID) key

Firebase console → ⚙️ **Project settings** → **Cloud Messaging** tab →
**Web configuration** → **Web Push certificates** → **Generate key pair**.

Copy the **Key pair** string and paste it into `src/common/firebaseConfig.ts`:

```ts
export const vapidKey = "PASTE_THE_KEY_PAIR_STRING_HERE";
```

Commit and redeploy the site (Vercel). Until this is set, the app shows
"Push notifications aren't set up yet" and everything else keeps working.

### 3. Publish the Firestore rules

Firebase console → **Firestore Database** → **Rules** → paste the contents of
`firestore.rules` → **Publish**. (Or `firebase deploy --only firestore:rules`.)

### 4. Deploy the Cloud Function

Install the Firebase CLI if you don't have it, then from the repo root:

```bash
npm install -g firebase-tools     # once
firebase login                    # once
firebase use zengmcloud-4a454     # select the project (or `firebase use --add`)
cd functions && npm install && cd ..
firebase deploy --only functions
```

That deploys `sendLeagueNotification`. Enabling FCM / Cloud Functions APIs is
automatic on first deploy.

### 5. On each phone, turn it on

1. Open the deployed site on the phone.
2. **iPhone only:** tap **Share → Add to Home Screen**, then open ZenGM from the
   new icon. iOS only allows web push for installed PWAs — a normal Safari tab
   will not work. (Android works in the browser directly.)
3. Load your league → **Tools → Multiplayer Sync** → connect to the room.
4. In the **Phone notifications** card, enter your name and tap
   **Enable phone notifications**, and accept the browser prompt.

Do this on every device that should receive pushes.

---

## Testing it

With two devices in the same room (one host), have the host sim a day, or make a
trade on one device. The other device should get a push within a few seconds —
background the app or lock the phone to confirm it fires while closed.

If nothing arrives, check, in order:

- **VAPID key set and redeployed?** (`vapidKey` non-empty, site rebuilt.)
- **Function deployed?** Firebase console → **Functions** shows
  `sendLeagueNotification`; its logs show each invocation.
- **Token registered?** Firestore → `leagues/{yourCode}/members` has a doc per
  device with an `fcmToken`.
- **iPhone installed to Home Screen?** Web push does nothing in a plain Safari tab.
- **Permission granted?** If you tapped "Block", re-enable notifications for the
  site in the browser/OS settings.

## Notes / knobs

- **Noise:** v1 pings everyone in the room (except the author) for every
  qualifying event. The `members` docs store each device's `tid` and the
  Cloud Function already supports per-team targeting via a notification's
  `targetTids` — set it in `buildNotification` (`notifications.ts`) if you later
  want, say, trades to ping only the involved GMs.
- **Names after a refresh:** your display name is restored the next time you open
  the Multiplayer Sync page. Until then, a notification you generate may read
  "A league-mate".
- **The change log still grows unbounded** (pre-existing) — unrelated to push,
  but worth a periodic manual cleanup of old `changes` docs for cost.
