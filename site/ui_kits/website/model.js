(function(){
"use strict";
var M=window.MSite;if(!M)return;var C=M.COL;
var root=document.getElementById("model");if(!root)return;var cv=root.querySelector("canvas");if(!cv||!cv.getContext)return;
var ctx=cv.getContext("2d"),boxes=[];
function B(x,y,z,w,h,d,kind,tag){boxes.push({x:x,y:y,z:z,w:w,h:h,d:d,kind:kind,range:tag.range||0,seg:tag.seg||"",rec:!!tag.rec,alt:!!tag.alt})}
var seed=7;function rnd(){seed=(seed*16807)%2147483647;return seed/2147483647}
var TY=0.02,PH=0.008,RH=0.1,RD=0.22;
function segment(x,len,z,range,id,sealed,n,fill,pick){var t={range:range,seg:id};B(x+len/2,TY+PH/2,z-0.03,len,PH,0.3,sealed?"sealed":"active",t);
var sizes=[],sum=0;for(var i=0;i<n;i++){var s=0.4+rnd()*rnd()*1.6;sizes.push(s);sum+=s}var used=sealed?len:len*fill,k=used/sum,p=x;
sizes.forEach(function(s,i){var w=s*k;B(p+w/2,TY+PH+RH/2,z-0.03,Math.max(0.004,w-0.006),RH,RD,i===pick?"pick":"record",{range:range,seg:id,rec:i===pick,alt:i%2===1});p+=w});
if(!sealed){var zl=len-used;B(p+zl/2,TY+PH+RH*0.25,z-0.03,zl-0.006,RH*0.5,RD,"zeros",t);B(p,TY+PH+(RH+0.06)/2,z+0.09,0.008,RH+0.06,0.008,"accent",t)}}
function range(id,x0,z,lo,hi,segs){var len=0,g=0.05;segs.forEach(function(s,i){len+=s.len+(i?g:0)});len+=0.1;var t={range:id};
B(x0+len/2,TY/2,z,len,TY,0.5,"tray",t);var p=x0+0.05;segs.forEach(function(s,i){if(i)p+=g;segment(p,s.len,z,id,s.id,s.sealed,s.n,s.fill||1,s.pick==null?-1:s.pick);p+=s.len});
var kw=Math.min(0.05,(len-0.1)/16);for(var k=0;k<16;k++){var own=k>=lo&&k<hi,h=own?0.022:0.008;B(x0+0.05+(k+0.5)*kw,TY+h/2,z+0.2,kw-0.006,h,0.04,own?"owned":"key",t)}return len}
var l1=range(1,-1.6,0,0,16,[{id:"r1a",len:0.5,sealed:true,n:10},{id:"r1b",len:0.5,sealed:true,n:10}]);
var FX=-1.6+l1,XC=FX+0.26;
range(2,XC,-0.3,0,8,[{id:"r2a",len:0.36,sealed:true,n:8},{id:"r2b",len:0.36,sealed:true,n:8},{id:"r2c",len:0.62,n:8,fill:0.5}]);
range(3,XC,0.3,8,16,[{id:"r3a",len:0.36,sealed:true,n:8},{id:"r3b",len:0.36,sealed:true,n:8},{id:"r3c",len:0.62,n:9,fill:0.58,pick:5}]);
var fy=TY+0.012;B(FX+0.065,fy,0,0.13,0.012,0.012,"ink",{});B(FX+0.13,fy,0,0.012,0.012,0.612,"ink",{});B(FX+0.195,fy,-0.3,0.13,0.012,0.012,"ink",{});B(FX+0.195,fy,0.3,0.13,0.012,0.012,"ink",{});B(FX+0.13,fy+0.01,0,0.03,0.03,0.03,"accent",{});
var XMAX=XC+1.48;B((-1.6+XMAX)/2,-0.008,0,XMAX+1.6+0.08,0.016,1.2,"base",{});
var MID=(-1.6+XMAX)/2,R=0;
boxes.forEach(function(b){b.x-=MID});boxes.forEach(function(b){R=Math.max(R,Math.hypot(Math.abs(b.x)+b.w/2,Math.abs(b.z)+b.d/2))});
var yaw=-0.55,pitch=0.62,level="topic";
function alphaOf(b){if(level==="topic")return 1;if(b.kind==="base")return 0.45;
if(level==="range")return b.range===3?1:0.2;if(level==="segment")return b.seg==="r3c"?1:0.18;if(level==="record")return b.rec?1:0.14;return 1}
function colorOf(b){switch(b.kind){case"base":return C["--muted"];case"tray":return C["--border"];case"sealed":return C["--state-sealed"];case"active":return C["--state-active"];
case"record":return b.alt?C["--ring"]:C["--muted-foreground"];case"pick":return level==="record"?C["--brand-accent"]:C["--muted-foreground"];case"zeros":return C["--card"];case"owned":case"ink":return C["--foreground"];case"key":return C["--input"];case"accent":return C["--brand-accent"];}return C["--muted-foreground"]}
var FACES=[[[0,1,3,2],[-1,0,0]],[[4,6,7,5],[1,0,0]],[[0,4,5,1],[0,-1,0]],[[2,3,7,6],[0,1,0]],[[0,2,6,4],[0,0,-1]],[[1,5,7,3],[0,0,1]]];
var W=0,Hh=0,dpr=1,raf=0;
function layout(){var r=cv.getBoundingClientRect();dpr=Math.min(2,window.devicePixelRatio||1);W=r.width;Hh=r.height;cv.width=Math.max(1,Math.round(W*dpr));cv.height=Math.max(1,Math.round(Hh*dpr))}
function draw(){raf=0;if(!W)return;ctx.setTransform(dpr,0,0,dpr,0,0);ctx.clearRect(0,0,W,Hh);
var cy=Math.cos(yaw),sy=Math.sin(yaw),cp=Math.cos(pitch),sp=Math.sin(pitch),s=Math.min(W/(2*R*0.9),Hh/(2*R*0.52)),ox=W/2,oy=Hh/2+s*0.08;
function rot(x,y,z){var xr=x*cy+z*sy,zr=-x*sy+z*cy;return[xr,y*cp-zr*sp,y*sp+zr*cp]}
var list=boxes.map(function(b){var c=rot(b.x,b.y,b.z);return{b:b,depth:c[2],layer:Math.round((b.y-b.h/2)*1000)}});
list.sort(function(a,b){return a.layer-b.layer||a.depth-b.depth});
list.forEach(function(it){var b=it.b,hw=b.w/2,hh=b.h/2,hd=b.d/2,pts=[];for(var i=0;i<8;i++){var p=rot(b.x+(i&4?hw:-hw),b.y+(i&2?hh:-hh),b.z+(i&1?hd:-hd));pts.push([ox+s*p[0],oy-s*p[1]])}
var col=colorOf(b),a=alphaOf(b);ctx.globalAlpha=a;
FACES.forEach(function(f){var n=rot(f[1][0],f[1][1],f[1][2]);if(n[2]<=0.001)return;ctx.beginPath();f[0].forEach(function(k,j){var q=pts[k];if(j)ctx.lineTo(q[0],q[1]);else ctx.moveTo(q[0],q[1])});ctx.closePath();
ctx.fillStyle=col;ctx.fill();var shade=f[1][1]===1?0:(Math.abs(n[0])>Math.abs(n[1])?0.3:0.16);if(shade){ctx.fillStyle="rgba(0,0,0,"+shade+")";ctx.fill()}})});
ctx.globalAlpha=1}
var started=false;function req(){if(started)draw()}
var drag=null;
cv.addEventListener("pointerdown",function(e){drag={x:e.clientX,y:e.clientY};cv.setPointerCapture(e.pointerId)});
cv.addEventListener("pointermove",function(e){if(!drag)return;yaw+=(e.clientX-drag.x)*0.008;pitch=Math.max(0.12,Math.min(1.25,pitch+(e.clientY-drag.y)*0.006));drag={x:e.clientX,y:e.clientY};req()});
["pointerup","pointercancel"].forEach(function(n){cv.addEventListener(n,function(){drag=null})});
cv.addEventListener("keydown",function(e){var k=e.key;if(k==="ArrowLeft")yaw-=0.15;else if(k==="ArrowRight")yaw+=0.15;else if(k==="ArrowUp")pitch=Math.min(1.25,pitch+0.1);else if(k==="ArrowDown")pitch=Math.max(0.12,pitch-0.1);else return;e.preventDefault();req()});
var fs=root.querySelector(".levels"),panels=[].slice.call(root.querySelectorAll(".level"));
function setLevel(v){level=v;panels.forEach(function(p){p.hidden=p.getAttribute("data-level")!==v});req()}
if(fs){fs.hidden=false;fs.addEventListener("change",function(e){if(e.target.name==="level")setLevel(e.target.value)})}
function start(){started=true;setLevel("topic");layout();req()}
M.whenNear(root,start);
if("ResizeObserver" in window)new ResizeObserver(function(){if(started){layout();req()}}).observe(cv);
M.onTheme(req);
})();
