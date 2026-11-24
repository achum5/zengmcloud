# Scores linesAudit.test.ts output: python3 tools/sportsbookAudit.py audit.jsonl ...
# (game lines by stage, ML/spread calibration, futures, award and team-award boards).
import json, math, sys, glob
from collections import defaultdict
import numpy as np

files = sys.argv[1:]
S = []
for f in files:
    for p in glob.glob(f):
        for l in open(p):
            if l.strip():
                S.append(json.loads(l))
print("seasons", len(S))

def imp(a):
    return 100 / (a + 100) if a > 0 else -a / (-a + 100)
def dec(a):
    return a / 100 + 1 if a > 0 else 100 / -a + 1

# ---------------- game lines ----------------
def stage(q):
    if q['po']:
        return 'playoffs'
    gp = min(q['hs'][0], q['as'][0])
    if gp == 0: return 'gp0'
    if gp <= 5: return 'gp1-5'
    if gp <= 15: return 'gp6-15'
    if gp <= 40: return 'gp16-40'
    return 'gp41+'

rows = defaultdict(list)
for s in S:
    res = {int(k): v for k, v in s['results'].items()}
    for q in s['quotes']:
        if q['gid'] not in res:
            continue
        h, a, ot = res[q['gid']]
        rows[q['ahead']].append((q, h, a, ot))

def report(rs, label):
    by = defaultdict(list)
    for r in rs:
        by[stage(r[0])].append(r)
        by['ALL'].append(r)
    print(f"\n=== game lines {label} ===")
    print(f"{'stage':10s} {'n':>6s} {'mBias':>6s} {'mRMSE':>6s} {'cover':>6s} {'mlLL':>6s} {'tBias':>6s} {'tRMSE':>6s} {'over':>6s} {'ROIund':>7s} {'ROIfav':>7s} {'ROIdog':>7s} {'ROIhomeS':>8s}")
    for k in ['gp0','gp1-5','gp6-15','gp16-40','gp41+','playoffs','ALL']:
        rr = by.get(k, [])
        if not rr: continue
        mres = [ (h-a) - q['margin'] for q,h,a,ot in rr]
        tres = [ (h+a) - q['xTotal'] for q,h,a,ot in rr]
        cov = [ 1 if (h-a)+q['spread']>0 else 0 for q,h,a,ot in rr if (h-a)+q['spread']!=0]
        ov = [ 1 if (h+a)>q['total'] else 0 for q,h,a,ot in rr if (h+a)!=q['total']]
        ll=[]; rf=[]; rd=[]
        for q,h,a,ot in rr:
            ph, pa = imp(q['ml'][0]), imp(q['ml'][1])
            p = ph/(ph+pa)
            hw = 1 if h>a else 0
            ll.append(-math.log(p if hw else 1-p))
            fav = 0 if q['ml'][0] < q['ml'][1] else 1
            favwon = (hw==1) if fav==0 else (hw==0)
            rf.append(dec(q['ml'][fav])-1 if favwon else -1)
            rd.append(dec(q['ml'][1-fav])-1 if not favwon else -1)
        und = [ (100/110) if (h+a)<q['total'] else -1 for q,h,a,ot in rr if (h+a)!=q['total']]
        hs = [ (100/110) if (h-a)+q['spread']>0 else -1 for q,h,a,ot in rr if (h-a)+q['spread']!=0]
        print(f"{k:10s} {len(rr):6d} {np.mean(mres):6.2f} {np.sqrt(np.mean(np.square(mres))):6.2f} {np.mean(cov):6.3f} {np.mean(ll):6.4f} {np.mean(tres):6.2f} {np.sqrt(np.mean(np.square(tres))):6.2f} {np.mean(ov):6.3f} {np.mean(und):7.3f} {np.mean(rf):7.3f} {np.mean(rd):7.3f} {np.mean(hs):8.3f}")

for ahead in sorted(rows):
    report(rows[ahead], f"quoted {ahead} day(s) ahead")

# ML calibration, game day
rr = rows[0]
print("\nML calibration (game day, de-vigged home prob bins)")
bins = defaultdict(lambda: [0,0,0.0])
for q,h,a,ot in rr:
    ph, pa = imp(q['ml'][0]), imp(q['ml'][1]); p = ph/(ph+pa)
    pf = max(p,1-p); fw = (h>a) if p>=0.5 else (a>h)
    b = min(9, int((pf-0.5)*20))
    bins[b][0]+=1; bins[b][1]+= 1 if fw else 0; bins[b][2]+=pf
for b in sorted(bins):
    n,w,ps = bins[b]
    print(f"  fav {0.5+b/20:.2f}-{0.55+b/20:.2f}: n={n:5d} priced={ps/n:.3f} won={w/n:.3f}")

# Spread cover by line size
print("\nSpread: cover rate of the favourite by line size (game day)")
bb = defaultdict(list)
for q,h,a,ot in rr:
    line = q['spread']
    fav_home = line < 0
    m = (h-a) if fav_home else (a-h)
    L = abs(line)
    if m == L: continue
    k = '0-3' if L<3.5 else '3.5-7' if L<7.5 else '7.5-12' if L<12.5 else '12.5+'
    bb[k].append(1 if m > L else 0)
for k in ['0-3','3.5-7','7.5-12','12.5+']:
    if bb[k]: print(f"  {k:7s} n={len(bb[k]):5d} favCover={np.mean(bb[k]):.3f}")

# ---------------- futures ----------------
def lp(lst, winner):
    s = sum(imp(a) for _, a in lst)
    for t, a in lst:
        if t == winner:
            return -math.log(max(1e-4, imp(a)/s)), imp(a)/s
    return -math.log(1e-4), 0.0

tags = []
for s in S:
    for b in s['boards']:
        if b['tag'] not in tags: tags.append(b['tag'])
print("\n=== futures ===")
print(f"{'tag':11s} {'n':>3s} {'titleLL':>7s} {'favP':>5s} {'favW':>5s} {'confLL':>6s} {'divLL':>6s} {'wMiss':>6s} {'wSlope':>6s} {'wOver':>5s} {'titleROI':>8s} {'missingChamp':>4s}")
for tag in tags:
    tl=[]; fav=[]; cl=[]; dl=[]; wins=[]; troi=[]; miss=0; n=0
    for s in S:
        b = next((x for x in s['boards'] if x['tag']==tag), None)
        if not b: continue
        n+=1
        out = {o['tid']: o for o in s['outcome']}
        champ = next((o['tid'] for o in s['outcome'] if o['prw']==s['rounds']), None)
        if b['title']:
            l,p = lp(b['title'], champ); tl.append(l)
            if p==0: miss+=1
            ssum = sum(imp(a) for _,a in b['title'])
            t0,a0 = min(b['title'], key=lambda r: r[1])
            fav.append((imp(a0)/ssum, 1 if t0==champ else 0))
            for t,a in b['title']:
                troi.append(dec(a)-1 if t==champ else -1)
        for cid, lst in b['conf']:
            w = next((o['tid'] for o in s['outcome'] if o['cid']==cid and o['prw']>=s['rounds']-1), None)
            cl.append(lp(lst, w)[0])
        for did, lst in b['div']:
            ind = [o for o in s['outcome'] if o['did']==did]
            w = max(ind, key=lambda o: o['winp'])['tid']
            dl.append(lp(lst, w)[0])
        for tid, line, ov, un in b['wins']:
            wins.append((line, out[tid]['won']))
    if n==0: continue
    if wins:
        L = np.array([w[0] for w in wins]); A = np.array([w[1] for w in wins])
        slope = np.cov(L, A)[0,1]/np.var(L) if np.var(L)>0 else float('nan')
        wm = np.sqrt(np.mean((A-L)**2)); wo = np.mean(A>L)
    else:
        slope=wm=wo=float('nan')
    f = lambda x: np.mean(x) if x else float('nan')
    print(f"{tag:11s} {n:3d} {f(tl):7.3f} {f([x[0] for x in fav]):5.3f} {f([x[1] for x in fav]):5.3f} {f(cl):6.3f} {f(dl):6.3f} {wm:6.2f} {slope:6.3f} {wo:5.3f} {f(troi):8.3f} {miss:4d}")

# title ROI by price bucket
print("\ntitle ROI by price bucket (all tags)")
bk = defaultdict(list)
for s in S:
    champ = next((o['tid'] for o in s['outcome'] if o['prw']==s['rounds']), None)
    for b in s['boards']:
        for t,a in b['title']:
            k = 'fav<=+300' if a<=300 else '+305..+1500' if a<=1500 else '+1505..+5000' if a<=5000 else '>+5000'
            bk[k].append(dec(a)-1 if t==champ else -1)
for k in ['fav<=+300','+305..+1500','+1505..+5000','>+5000']:
    if bk[k]: print(f"  {k:14s} n={len(bk[k]):5d} ROI={np.mean(bk[k]):.3f}")

# ---------------- awards ----------------
print("\n=== awards (winner's de-vigged prob; 'off' = winner not on board) ===")
for tag in tags:
    line = []
    for aw in ['mvp','dpoy','roy','smoy','mip']:
        ll=[]; off=0; n=0; roi=[]
        for s in S:
            if not s.get('awards'): continue
            w = s['awards']['individual'].get(aw)
            b = next((x for x in s['boards'] if x['tag']==tag), None)
            if not b or w is None: continue
            lst = next((c for k,c in b['awards'] if k==aw), None)
            if not lst: continue
            n+=1
            l,p = lp(lst, w); ll.append(l)
            if p==0: off+=1
            for pid,a in lst: roi.append(dec(a)-1 if pid==w else -1)
        if n: line.append(f"{aw}:LL{np.mean(ll):.2f}/off{off}/{n}/roi{np.mean(roi):+.2f}")
    if line: print(f"{tag:11s} " + "  ".join(line))

print("\n=== All-Star board (made-it rate vs priced, off = all-star not on board) ===")
for tag in tags:
    pr=[]; hit=[]; off=0; tot=0; roi=[]
    for s in S:
        b = next((x for x in s['boards'] if x['tag']==tag), None)
        if not b or not b['allStar'] or not s.get('allStars'): continue
        made = set(s['allStars']); on = {pid for pid,_ in b['allStar']}
        off += len(made - on); tot += len(made)
        for pid,a in b['allStar']:
            pr.append(imp(a)); hit.append(1 if pid in made else 0)
            roi.append(dec(a)-1 if pid in made else -1)
    if pr: print(f"{tag:11s} rows={len(pr):5d} priced={np.mean(pr):.3f} hit={np.mean(hit):.3f} roi={np.mean(roi):+.3f} offBoard={off}/{tot}")

for kind, key in [('All-League', 'allLeague'), ('All-Defensive', 'allDefensive')]:
    print(f"\n=== {kind} tiers ===")
    for tag in tags:
        pr=[]; hit=[]; roi=[]; off=0; tot=0
        for s in S:
            b = next((x for x in s['boards'] if x['tag']==tag), None)
            if not b or not b[key] or not s.get('awards'): continue
            for tier, lst in b[key]:
                made = set(s['awards'][key][tier-1]) if len(s['awards'][key])>=tier else set()
                on = {pid for pid,_ in lst}
                off += len(made-on); tot += len(made)
                for pid,a in lst:
                    pr.append(imp(a)); hit.append(1 if pid in made else 0); roi.append(dec(a)-1 if pid in made else -1)
        if pr: print(f"{tag:11s} rows={len(pr):5d} priced={np.mean(pr):.3f} hit={np.mean(hit):.3f} roi={np.mean(roi):+.3f} offBoard={off}/{tot}")
