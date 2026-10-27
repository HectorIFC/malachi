(function(){
"use strict";
var M=window.MSite;if(!M)return;var C=M.COL,TAU=Math.PI*2;
function E(x){return x<=0?0:x>=1?1:x*x*(3-2*x)}
function P(t,a,b){return E((t-a)/(b-a))}
function L(a,b,p){return a+(b-a)*p}
function H(i){var x=Math.sin(i*12.9898+4.1)*43758.5453;return x-Math.floor(x)}
function dot(c,x,y,r,col,a){c.globalAlpha=a==null?1:a;c.fillStyle=col;c.beginPath();c.arc(x,y,r,0,TAU);c.fill()}
function ring(c,x,y,r,col,a){c.globalAlpha=a==null?1:a;c.strokeStyle=col;c.lineWidth=1.4;c.beginPath();c.arc(x,y,r,0,TAU);c.stroke()}
function sq(c,x,y,s,col,a){c.globalAlpha=a==null?1:a;c.fillStyle=col;c.fillRect(x-s/2,y-s/2,s,s)}
function rr(c,x,y,w,h,r){c.beginPath();if(c.roundRect)c.roundRect(x,y,w,h,r);else c.rect(x,y,w,h)}
function box(c,x,y,w,h,r,fill,stroke,a){c.globalAlpha=a==null?1:a;rr(c,x,y,w,h,r);if(fill){c.fillStyle=fill;c.fill()}if(stroke){c.strokeStyle=stroke;c.lineWidth=1;c.stroke()}}
function ln(c,x1,y1,x2,y2,col,w,dash,a){c.globalAlpha=a==null?1:a;c.strokeStyle=col;c.lineWidth=w||1;c.setLineDash(dash||[]);c.beginPath();c.moveTo(x1,y1);c.lineTo(x2,y2);c.stroke();c.setLineDash([])}
function mount(fig,W,H_,LOOP,STILL,fn){var cv=fig.querySelector("canvas");if(!cv||!cv.getContext)return;var g=M.stage(cv,W,H_);
var sc=M.Scene({el:fig,hit:fig.querySelector(".scene-box"),frame:function(t){g.clear();fn(g.ctx,t%LOOP);g.ctx.globalAlpha=1},still:function(){g.clear();fn(g.ctx,STILL);g.ctx.globalAlpha=1}});g.cb=sc.redraw}

/* Hero hook: three stories rotate; each has its own scene. Reduced motion: story 1, all lines shown. */
function hook(fig){
var viz=fig.querySelector(".hook-viz");if(!viz)return;
var stories=[].slice.call(fig.querySelectorAll(".story")),panes=[].slice.call(viz.querySelectorAll(".viz"));
var ncv=[].slice.call(panes[0].querySelectorAll("canvas"));if(!ncv.length||!ncv[0].getContext)return;
fig.classList.add("is-live");
var ng=ncv.map(function(c){return M.stage(c,200,96)}),sg=M.stage(panes[1].querySelector("canvas"),640,136),rg=M.stage(panes[2].querySelector("canvas"),640,136);
var n2=panes[0].querySelector('[data-node="2"]'),shape=n2.querySelector("[data-shape]"),word=n2.querySelector("[data-word]");
var ST=9,LN=[1.5,2.7,3.9],OUT=8.4,cur=-1,sc=null;
function show(i){if(i===cur)return;cur=i;stories.forEach(function(s,j){s.classList.toggle("on",j===i)});panes.forEach(function(p,j){p.classList.toggle("on",j===i)});viz.setAttribute("data-desc",panes[i].getAttribute("data-desc"));if(sc)sc.relabel()}
function lines(i,t){[].forEach.call(stories[i].querySelectorAll("span"),function(s,k){s.classList.toggle("on",t>=LN[k]&&t<OUT)})}
var IV=0.45,FAIL=1.2,BEH=5.4,OK=6.8;
function state(t){return t<FAIL?"active":t<BEH?"unknown":t<OK?"behind":"active"}
function rows(c,cnt,live,dim){var n=Math.floor(cnt),f=E((cnt-n)/0.55);for(var k=0;k<6;k++){var j=n-k;if(j<0)continue;var y=6+(k-1+f)*18;
box(c,10,y,180,13,3,C["--muted"],k===0&&live&&f<1?C["--brand-accent"]:C["--border"],dim);
dot(c,21,y+6.5,3,C["--state-active"],dim*(live?1:0.6));box(c,32,y+4.5,40+H(j)*110,4,2,C["--muted-foreground"],null,dim*0.55)}}
function nodes(t){var s=state(t);if(word.textContent!==s){word.textContent=s;shape.className="sh sh-"+s;n2.classList.toggle("is-down",s==="unknown")}
var live=t/IV+4,c2;if(t<FAIL)c2=live;else if(t<BEH)c2=FAIL/IV+4;else if(t<OK)c2=L(FAIL/IV+4,live,P(t,BEH,OK));else c2=live;
ng.forEach(function(g,i){g.clear();var c=g.ctx;if(i===1){var dead=t>=FAIL&&t<BEH;rows(c,c2,!dead,dead?0.35:1)}else rows(c,live+i*0.31,true,1);c.globalAlpha=1})}
function splitS(c,t){var fa=1-P(t,8.4,9),SP=1.6,sealed=t>=SP;
box(c,16,10,608,44,8,null,sealed?C["--state-sealed"]:C["--state-active"],fa*0.8);
var n=Math.min(24,Math.floor(t/SP*24));for(var i=0;i<n;i++){var x=34+i*24.5;if(sealed)sq(c,x,32,9,C["--state-sealed"],fa);else dot(c,x,32,4.5,C["--state-active"],fa)}
ln(c,320,4,320,60,C["--brand-accent"],2,null,fa*P(t,SP-0.4,SP)*(1-P(t,SP+1.4,SP+2)));
var ca=P(t,SP+0.2,SP+0.7);ln(c,320,54,320,82,C["--muted-foreground"],1,[2,3],fa*P(t,SP+1.4,SP+2));
ln(c,168,54,168,82,C["--muted-foreground"],1,[2,3],fa*ca);ln(c,472,54,472,82,C["--muted-foreground"],1,[2,3],fa*ca);
box(c,16,82,298,44,8,null,C["--state-active"],fa*ca*0.8);box(c,326,82,298,44,8,null,C["--state-active"],fa*ca*0.8);
var m=Math.max(0,Math.min(11,Math.floor((t-SP-0.8)*3)));for(var j=0;j<m;j++){dot(c,34+j*25.5,104,4.5,C["--state-active"],fa);dot(c,344+j*25.5,104,4.5,C["--state-active"],fa)}}
function restartS(c,t){var fa=1-P(t,8.4,9),Y=[26,68,110],R0=1.2,R1=4.8,X0=[110,140,170];
Y.forEach(function(y,i){var down=i===1&&t>=R0&&t<R1;if(down)ring(c,36,y,7,C["--state-unknown"],fa);else dot(c,36,y,7,C["--state-active"],fa);
ln(c,50,y,96,y,C["--border"],1,null,fa);
var te=i!==1||t<R0?t:t<R1?R0:t-(R1-R0),x=Math.min(618,X0[i]+50*te);
for(var k=0;k<25;k++){var rx=110+k*21;dot(c,rx,y,3,C["--muted-foreground"],fa*(rx<x?0.35:0.9))}
ln(c,x,y-14,x,y+14,C["--brand-accent"],2,down?[3,3]:null,fa);dot(c,x,y-16,3,C["--brand-accent"],fa)})}
function frame(T,still){var i,t;if(still){i=0;t=4.5}else{T%=ST*3;i=Math.floor(T/ST);t=T-i*ST}show(i);lines(i,t);
if(i===0)nodes(t);else{var g=i===1?sg:rg;g.clear();(i===1?splitS:restartS)(g.ctx,t);g.ctx.globalAlpha=1}}
sc=M.Scene({el:fig,hit:viz,frame:function(T){frame(T)},still:function(){frame(0,true)}});
ng.concat([sg,rg]).forEach(function(g){g.cb=sc.redraw})}

/* How 1: a busy range splits at the midpoint of its keyspace. */
function split(fig){mount(fig,600,220,9,7,function(c,t){
var CW=33.75,ps=P(t,3.6,4.2),fa=1-P(t,8.4,9);
function x(k){return 22+k*CW+(k>=8?8:0)+(k>=12?8*ps:0)}function cx(k){return x(k)+CW/2}
for(var k=0;k<16;k++)box(c,x(k)+2,178,CW-4,10,2,C["--border"],null,fa);
var outl=t<3.6?[[0,8],[8,16]]:[[0,8],[8,12],[12,16]];outl.forEach(function(r){box(c,x(r[0])-2,172,x(r[1]-1)+CW-x(r[0])+4,22,5,null,C["--muted-foreground"],fa)});
var nA=Math.min(24,Math.floor(t*2.2));for(var i=0;i<nA;i++)dot(c,cx(i%8),160-Math.floor(i/8)*13,4,C["--state-active"],fa);
var nB=Math.min(40,Math.floor(Math.min(t,3.6)*11)),sealed=t>=3.6;
for(i=0;i<nB;i++){var kx=cx(8+i%8),ky=160-Math.floor(i/8)*13;if(sealed)sq(c,kx,ky,7,C["--state-sealed"],fa);else dot(c,kx,ky,4,C["--state-active"],fa)}
[[8,0],[12,0.17]].forEach(function(h){var n=t>3.9?Math.min(16,Math.floor((t-3.9-h[1])*5)):0;for(var j=0;j<n;j++)dot(c,cx(h[0]+j%4),160-(5+Math.floor(j/4))*13,4,C["--state-active"],fa)});
var fx=(x(11)+CW+x(12))/2,fv=P(t,2.8,3.3)*(1-P(t,4.6,5.2));if(fv>0)ln(c,fx,196,fx,L(196,34,P(t,2.8,3.3)),C["--brand-accent"],2,null,fv*fa);
if(t>=4.6)ln(c,fx,40,fx,168,C["--muted-foreground"],1,[2,3],P(t,4.6,5.2)*fa)})}

/* How 2: primary ships to two followers; ack after a majority; a full segment seals on every copy. */
function replicate(fig){mount(fig,600,220,10,9,function(c,t){
var Y=[50,110,170],fa=1-P(t,9.4,10),D=[0,0.2,0.42];function ti(i){return 0.3+i*0.34+(i>=10?0.7:0)}var ts0=ti(9)+0.42+0.25;
dot(c,36,Y[0],7,C["--foreground"],fa);ring(c,36,Y[1],7,C["--muted-foreground"],fa);ring(c,36,Y[2],7,C["--muted-foreground"],fa);
for(var s=0;s<2;s++){if(t<ti(s*10))continue;var sx=110+s*250,sl=s===0&&t>=ts0;for(var r=0;r<3;r++)box(c,sx-12,Y[r]-12,9*22+24,24,6,null,sl?C["--state-sealed"]:C["--state-active"],fa*(sl?1:0.7))}
for(var i=0;i<20;i++){var a=ti(i);if(t<a)break;var sg=Math.floor(i/10),x=110+sg*250+(i%10)*22,isS=sg===0&&t>=ts0;
for(r=0;r<3;r++){var at=a+D[r];if(r>0&&t>=a&&t<at){dot(c,x,L(Y[0],Y[r],(t-a)/D[r]),2.5,C["--brand-accent"],fa)}if(t<at)continue;
var al=fa*P(t,at,at+0.1);if(isS)sq(c,x,Y[r],8,C["--state-sealed"],al);else if(r===0&&t<a+D[1])ring(c,x,Y[r],4,C["--state-active"],al);else dot(c,x,Y[r],4.2,C["--state-active"],al)}}})}

/* How 3: a cursor reads the sealed parent, pauses, resumes, then follows its key into the child. */
function cursor(fig){mount(fig,600,220,9,7.6,function(c,t){
var fa=1-P(t,8.4,9),PY=56,Y1=120,Y2=176,cx,cy;
var c1=Math.min(8,2+Math.floor(t*0.9)),c2=Math.min(8,1+Math.floor(t*0.8)),head=370+(c1-1)*22+14;
if(t<0.5){cx=46;cy=PY}else if(t<2){cx=L(46,180,P(t,0.5,2));cy=PY}else if(t<3.2){cx=180;cy=PY}else if(t<4.4){cx=L(180,314,P(t,3.2,4.4));cy=PY}else if(t<4.6){cx=314;cy=PY}else{cx=L(358,head,P(t,4.6,6.6));cy=Y1}
box(c,46,PY-13,12*22+6,26,6,null,C["--state-sealed"],fa);for(var j=0;j<12;j++){var x=60+j*22;sq(c,x,PY,9,C["--state-sealed"],fa*(cy===PY?(x<cx?0.35:1):0.35))}
ln(c,318,PY+6,350,Y1,C["--muted-foreground"],1,[2,3],fa);ln(c,318,PY+6,350,Y2,C["--muted-foreground"],1,[2,3],fa);
[[Y1,c1],[Y2,c2]].forEach(function(r){box(c,356,r[0]-13,8*22+6,26,6,null,C["--state-active"],fa*0.7);for(var k=0;k<r[1];k++){var kx=370+k*22;dot(c,kx,r[0],4.5,C["--state-active"],fa*(r[0]===cy&&kx<cx?0.45:1))}});
var paused=t>=2&&t<3.2;ln(c,cx,cy-20,cx,cy+20,C["--brand-accent"],2,paused?[3,3]:null,fa);dot(c,cx,cy-22,3.5,C["--brand-accent"],fa)})}

/* vs scenes: left the compared system, right Malachi. */
function lanesBase(c,ys,x0,x1,fa,from){ys.forEach(function(y,i){ln(c,from&&from[i]?from[i]:x0,y,x1,y,C["--border"],1,null,fa)})}
function kafka(fig){mount(fig,600,240,9,8,function(c,t){
var fa=1-P(t,8.5,9),LY=[60,110,160,210],TS=3.4;ln(c,300,10,300,230,C["--border"],1,null,fa);
lanesBase(c,LY.slice(0,3),20,280,fa);ln(c,20+TS*34,210,280,210,C["--border"],1,null,fa*P(t,TS-0.2,TS+0.2));
ln(c,320,135,320+TS*34,135,C["--border"],1,null,fa);if(t>=TS){var fx=320+TS*34;ln(c,fx,135,fx+12,85,C["--muted-foreground"],1,[2,3],fa);ln(c,fx,135,fx+12,185,C["--muted-foreground"],1,[2,3],fa);ln(c,fx+12,85,580,85,C["--border"],1,null,fa);ln(c,fx+12,185,580,185,C["--border"],1,null,fa)}
for(var i=0;i<75;i++){var tr=0.2+i*0.1;if(tr>t)break;var k=Math.floor(H(i)*8),hot=k===5,lanes=tr<TS?3:4,lx=20+tr*34,rx=320+tr*34,ry=tr<TS?135:(k<4?85:185);
var col=hot?C["--brand-accent"]:C["--muted-foreground"],r=hot?3.4:2.2,a=fa*(hot?1:0.8);dot(c,lx,LY[k%lanes],r,col,a);
if(tr<TS&&t>=TS)sq(c,rx,ry,hot?6:4.4,hot?C["--brand-accent"]:C["--state-sealed"],a);else dot(c,rx,ry,r,hot?col:C["--state-active"],a)}})}
function redpanda(fig){mount(fig,600,240,10.5,9.6,function(c,t){
var fa=1-P(t,10,10.5),LY=[40,80,120,160,200],TS=2.6,TM=6.2;ln(c,300,10,300,230,C["--border"],1,null,fa);
lanesBase(c,LY.slice(0,3),20,280,fa);ln(c,20+TS*29,160,280,160,C["--border"],1,null,fa*P(t,TS-0.2,TS+0.2));ln(c,20+TS*29,200,280,200,C["--border"],1,null,fa*P(t,TS-0.2,TS+0.2));
var sx=320+TS*29,mx=320+TM*29;ln(c,320,120,sx,120,C["--border"],1,null,fa);
if(t>=TS){ln(c,sx,120,sx+10,70,C["--muted-foreground"],1,[2,3],fa);ln(c,sx,120,sx+10,170,C["--muted-foreground"],1,[2,3],fa);ln(c,sx+10,70,Math.min(mx,580),70,C["--border"],1,null,fa);ln(c,sx+10,170,Math.min(mx,580),170,C["--border"],1,null,fa)}
if(t>=TM){ln(c,mx,70,mx+10,120,C["--muted-foreground"],1,[2,3],fa);ln(c,mx,170,mx+10,120,C["--muted-foreground"],1,[2,3],fa);ln(c,mx+10,120,580,120,C["--border"],1,null,fa)}
for(var i=0;i<87;i++){var tr=0.2+i*0.1;if(tr>t)break;var busy=tr>=1.8&&tr<5.6;if(!busy&&H(i+7)>0.3)continue;var k=Math.floor(H(i)*8),lanes=tr<TS?3:5,lx=20+tr*29,rx=320+tr*29;
dot(c,lx,LY[k%lanes],2.2,C["--muted-foreground"],fa*0.85);
var ry=tr<TS?120:tr<TM?(k<4?70:170):120,seal=(tr<TS&&t>=TS)||(tr>=TS&&tr<TM&&t>=TM);if(seal)sq(c,rx,ry,4.4,C["--state-sealed"],fa);else dot(c,rx,ry,2.4,C["--state-active"],fa)}})}
function pulsar(fig){mount(fig,600,240,3,1.6,function(c,t){
ln(c,300,10,300,230,C["--border"],1,null,1);var BX=[70,150,230];
BX.forEach(function(x){ln(c,150,70,x,140,C["--border"],1);ln(c,x,70,150,205,C["--border"],1,[2,3])});
BX.forEach(function(x){box(c,x-12,58,24,24,12,C["--card"],C["--muted-foreground"]);box(c,x-10,130,20,20,3,C["--card"],C["--muted-foreground"])});
[110,150,190].forEach(function(x){box(c,x-7,198,14,14,3,null,C["--muted-foreground"])});dot(c,150,22,4,C["--foreground"]);
if(t<0.6)dot(c,150,L(26,58,P(t,0,0.6)),3,C["--brand-accent"]);else if(t<1.3)BX.forEach(function(x){dot(c,L(150,x,P(t,0.6,1.3)),L(82,130,P(t,0.6,1.3)),3,C["--brand-accent"])});
else if(t<2.1){var p=P(t,1.3,2.1);dot(c,150,L(82,198,p),2.6,C["--brand-accent"],1-p*0.3)}
var NX=[360,450,540];NX.forEach(function(x){box(c,x-32,82,64,84,8,C["--card"],C["--muted-foreground"]);for(var j=0;j<3;j++)dot(c,x-12+j*12,110,4,C["--state-active"]);[[-8,148],[8,148],[0,138]].forEach(function(d){ring(c,x+d[0],d[1],3,C["--muted-foreground"])})});
dot(c,450,22,4,C["--foreground"]);
if(t<0.6)dot(c,450,L(26,82,P(t,0,0.6)),3,C["--brand-accent"]);else if(t<1.3){var q=P(t,0.6,1.3);dot(c,L(450,360,q),124,3,C["--brand-accent"]);dot(c,L(450,540,q),124,3,C["--brand-accent"])}})}
var B={hook:hook,split:split,replicate:replicate,cursor:cursor,kafka:kafka,redpanda:redpanda,pulsar:pulsar};
function init(){document.querySelectorAll("[data-scene]").forEach(function(f){var b=B[f.getAttribute("data-scene")];if(b)b(f)})}
if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",init);else init();
})();
