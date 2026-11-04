# One possession of a game tape, as top-down panels of 1.5 s each (two to a row):
#
#   python3 view.py <tape.json> <possession> <out.png> [<from ms> <to ms>] [<pid>]
#
# Trails fade in over each panel; facing ticks and animation names at its last
# moment; the ball orange; <pid> drawn green; flags in each panel listed under it.
import json, sys, math
from PIL import Image, ImageDraw
tape=json.load(open(sys.argv[1])); pi=int(sys.argv[2]); out=sys.argv[3]
P=[p for p in tape['possessions'] if p['i']==pi][0]
fr=P['frames']
ta=int(sys.argv[4]) if len(sys.argv)>4 else fr[0]['t']; tb=int(sys.argv[5]) if len(sys.argv)>5 else fr[-1]['t']
focus=int(sys.argv[6]) if len(sys.argv)>6 else None
fr=[f for f in fr if ta<=f['t']<=tb]
team={p['pid']:p['team'] for p in tape['players']}
W,H=94,50; S=10; pad=12
# panels of 1.5 s
chunks=[]; cur=[]
for f in fr:
    cur.append(f)
    if f['t']-cur[0]['t']>=1500: chunks.append(cur); cur=[f]
if len(cur)>1: chunks.append(cur)
flags=P['flags']
imgs=[]
for ch in chunks:
    img=Image.new('RGB',(W*S+2*pad,H*S+2*pad+34),'white'); d=ImageDraw.Draw(img)
    X=lambda x: pad+x*S; Y=lambda y: pad+y*S
    d.rectangle([X(0),Y(0),X(W),Y(H)],outline=(150,150,150))
    d.line([X(W/2),Y(0),X(W/2),Y(H)],fill=(190,190,190))
    for rx,sgn in ((5.25,1),(W-5.25,-1)):
        d.ellipse([X(rx)-4,Y(25)-4,X(rx)+4,Y(25)+4],outline='orange')
        d.arc([X(rx)-23.75*S,Y(25)-23.75*S,X(rx)+23.75*S,Y(25)+23.75*S],-70 if sgn==1 else 110,70 if sgn==1 else 250,fill=(210,210,210))
        x0=0 if sgn==1 else W-19
        d.rectangle([X(x0),Y(17),X(x0+19),Y(33)],outline=(210,210,210))
    n=len(ch)
    for i,f in enumerate(ch):
        last=i==n-1
        for pid,x,y,yaw,anim,shown in f['p']:
            if not shown or y<-3: continue
            a=i/max(1,n-1)
            base=(40,90,230) if team[pid]==0 else (220,50,50)
            if pid==focus: base=(0,150,0)
            col=tuple(int(255-(255-c)*(0.25+0.75*a)) for c in base)
            r=1.6 if not last else 5
            d.ellipse([X(x)-r,Y(y)-r,X(x)+r,Y(y)+r],fill=col)
            if last:
                d.line([X(x),Y(y),X(x+math.cos(yaw)*1.6),Y(y+math.sin(yaw)*1.6)],fill='black',width=2)
                d.text((X(x)+6,Y(y)-6),f"{pid} {anim}",fill='black')
        bx,by,bz,h=f['b']
        d.ellipse([X(bx)-1.6,Y(by)-1.6,X(bx)+1.6,Y(by)+1.6],fill=(255,140,0))
    t0=ch[0]['t']; t1=ch[-1]['t']
    fl=[x for x in flags if t0-50<=x['t']<=t1+50]
    for x in fl:
        # mark flagged player at that time
        f=min(ch,key=lambda f:abs(f['t']-x['t']))
        for pid,px,py,_,_,_ in f['p']:
            if pid==x.get('pid'): d.ellipse([X(px)-9,Y(py)-9,X(px)+9,Y(py)+9],outline=(0,0,0),width=2)
    bt=[b for b in P['beats'] if t0-1500<=b['t']<=t1+1500]
    d.text((pad,H*S+2*pad-2),f"{t0}-{t1}  beats: "+", ".join(f"{b['type']}@{b['t']}" for b in bt)[:110],fill='black')
    d.text((pad,H*S+2*pad+14),("FLAGS: "+"; ".join(f"{x['kind']} {x['text']}@{x['t']}" for x in fl))[:140],fill=(180,0,0))
    imgs.append(img)
cols=2
rows=(len(imgs)+cols-1)//cols
w=imgs[0].width; h=imgs[0].height
tot=Image.new('RGB',(w*cols,h*rows),'white')
for k,im in enumerate(imgs): tot.paste(im,((k%cols)*w,(k//cols)*h))
tot.save(out); print(out, len(imgs),'panels')
