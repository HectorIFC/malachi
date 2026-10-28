(function(){
"use strict";
var root=document.documentElement,KEY="malachi-theme",mqDark=matchMedia("(prefers-color-scheme: dark)"),mqRM=matchMedia("(prefers-reduced-motion: reduce)");
function pref(){try{return localStorage.getItem(KEY)||"system"}catch(e){return "system"}}
function applyTheme(){var v=pref();root.classList.toggle("dark",v==="dark"||(v==="system"&&mqDark.matches));document.querySelectorAll('input[name="theme"]').forEach(function(i){i.checked=i.value===v})}
document.addEventListener("change",function(e){if(e.target.name==="theme"){try{localStorage.setItem(KEY,e.target.value)}catch(_){}applyTheme()}});
mqDark.addEventListener("change",applyTheme);
document.addEventListener("click",function(e){var b=e.target.closest("[data-copy]");if(!b)return;var t=document.getElementById(b.dataset.copy);if(!t||!navigator.clipboard)return;navigator.clipboard.writeText(t.textContent).then(function(){b.textContent="Copied";setTimeout(function(){b.textContent="Copy"},1600)})});
function tabs(){document.querySelectorAll("[data-tabs]").forEach(function(box){var list=box.querySelector('[role="tablist"]'),btns=[].slice.call(list.querySelectorAll('[role="tab"]'));var panels=btns.map(function(b){var p=document.getElementById(b.getAttribute("aria-controls"));p.setAttribute("role","tabpanel");p.setAttribute("aria-labelledby",b.id);p.tabIndex=0;return p});
list.hidden=false;box.classList.add("is-tabbed");
function sel(i,f){btns.forEach(function(b,j){var on=i===j;b.setAttribute("aria-selected",on?"true":"false");b.tabIndex=on?0:-1;panels[j].hidden=!on});if(f)btns[i].focus()}
btns.forEach(function(b,i){b.addEventListener("click",function(){sel(i)});b.addEventListener("keydown",function(e){var n=null,L=btns.length;if(e.key==="ArrowRight")n=(i+1)%L;else if(e.key==="ArrowLeft")n=(i-1+L)%L;else if(e.key==="Home")n=0;else if(e.key==="End")n=L-1;if(n!==null){e.preventDefault();sel(n,true)}})});sel(0)})}

/* Scene controller: runs only while visible and the tab is visible; click or Space pauses; static frame under reduced motion. */
function pausable(hit,onToggle){var st={paused:false,on:false};if(!hit)return{enable:function(){},relabel:function(){}};hit.setAttribute("role","img");
function lab(){var d=hit.getAttribute("data-desc")||"Animation";hit.setAttribute("aria-label",st.on?d+". "+(st.paused?"Paused. Click or press Space to play.":"Click or press Space to pause."):d+".")}
function tog(){if(!st.on)return;st.paused=!st.paused;hit.classList.toggle("is-paused",st.paused);hit.setAttribute("aria-pressed",String(st.paused));lab();onToggle(st.paused)}
hit.addEventListener("click",tog);hit.addEventListener("keydown",function(e){if(e.key===" "||e.key==="Enter"){e.preventDefault();tog()}});
return{enable:function(on){st.on=on;if(on){hit.tabIndex=0;hit.setAttribute("role","button");hit.setAttribute("aria-pressed",String(st.paused));hit.classList.add("is-pausable")}else{hit.removeAttribute("tabindex");hit.setAttribute("role","img");hit.removeAttribute("aria-pressed");hit.classList.remove("is-pausable")}lab()},relabel:lab}}
var COL={},scenes=[];
function readCols(){var cs=getComputedStyle(root);["--state-active","--state-sealed","--state-behind","--state-damaged","--state-unknown","--brand-accent","--muted-foreground","--border","--foreground","--card","--muted","--background","--ring","--input","--accent"].forEach(function(k){COL[k]=cs.getPropertyValue(k).trim()})}
readCols();
function Scene(o){var st={started:false,paused:false,visible:false,running:false,t:0,raf:0,last:0,ld:0},FPS=parseFloat(getComputedStyle(root).getPropertyValue("--scene-fps"))||30;
function draw(){o.frame(st.t/1000)}
function loop(now){st.raf=0;if(!st.running)return;st.t+=st.last?Math.min(100,now-st.last):0;st.last=now;if(now-st.ld>=1000/FPS-1){draw();st.ld=now}st.raf=requestAnimationFrame(loop)}
function stop(){st.running=false;if(st.raf)cancelAnimationFrame(st.raf);st.raf=0}
function sync(){var go=!mqRM.matches&&!st.paused&&st.visible&&!document.hidden;if(go&&!st.running){if(!st.started){st.started=true;st.t=(o.t0||0)*1000}st.running=true;st.last=0;st.raf=requestAnimationFrame(loop)}else if(!go&&st.running)stop()}
var hp=pausable(o.hit,function(p){st.paused=p;sync()});
function mode(){if(mqRM.matches){stop();hp.enable(false);o.still()}else{hp.enable(true);if(st.started)draw();else o.still();sync()}}
if("IntersectionObserver" in window)new IntersectionObserver(function(es){st.visible=es[es.length-1].isIntersecting;sync()}).observe(o.el);else st.visible=true;
var api={el:o.el,seek:function(sec){st.started=true;st.t=sec*1000;draw()},sync:sync,mode:mode,relabel:hp.relabel,redraw:function(){if(mqRM.matches||!st.started)o.still();else draw()}};scenes.push(api);mode();return api}
function stage(cv,W,H){var g={ctx:cv.getContext("2d"),W:W,H:H,cb:null};function lay(){var b=cv.getBoundingClientRect(),d=Math.min(2,window.devicePixelRatio||1);cv.width=Math.max(1,Math.round(b.width*d));cv.height=Math.max(1,Math.round(b.height*d));var k=Math.min(b.width/W,b.height/H)||1;g.k=k;g.ctx.setTransform(d*k,0,0,d*k,d*(b.width-W*k)/2,d*(b.height-H*k)/2)}
lay();if("ResizeObserver" in window)new ResizeObserver(function(){lay();if(g.cb)g.cb()}).observe(cv);
g.clear=function(){var d=Math.min(2,window.devicePixelRatio||1);if(Math.abs(cv.width-Math.round(cv.clientWidth*d))>1||Math.abs(cv.height-Math.round(cv.clientHeight*d))>1)lay();var c=g.ctx;c.save();c.setTransform(1,0,0,1,0,0);c.clearRect(0,0,cv.width,cv.height);c.restore()};return g}
document.addEventListener("visibilitychange",function(){scenes.forEach(function(s){s.sync()})});
mqRM.addEventListener("change",function(){scenes.forEach(function(s){s.mode()})});
new MutationObserver(function(){readCols();scenes.forEach(function(s){s.redraw()})}).observe(root,{attributes:true,attributeFilter:["class"]});
window.MSite={scenes:scenes,Scene:Scene,stage:stage,COL:COL,rm:mqRM,onTheme:function(f){new MutationObserver(function(){readCols();f()}).observe(root,{attributes:true,attributeFilter:["class"]})}};

/* Thesis canvas: seeded simulation of one topic's keyspace. K key rows; ranges are buddy blocks. */
function thesis(){
var cv=document.querySelector("#thesis canvas");if(!cv||!cv.getContext)return;
var ctx=cv.getContext("2d");
var cs=getComputedStyle(root),TICK=parseFloat(cs.getPropertyValue("--thesis-tick"))||140,FPS=parseFloat(cs.getPropertyValue("--thesis-fps"))||30;
var K=32,MIN=4,SEGMAX=46,PERIOD=560,STATIC_T=220;
var C={};function colors(){var s=getComputedStyle(root);["--state-active","--state-sealed","--muted-foreground","--border"].forEach(function(k){C[k]=s.getPropertyValue(k).trim()})}
var W=[],WT=0;for(var r=0;r<K;r++){var w=0.55+1.2*Math.pow(Math.sin((r/K)*Math.PI*1.15+0.35),2);W.push(w);WT+=w}
function wsum(lo,size){var s=0;for(var i=lo;i<lo+size;i++)s+=W[i];return s}
var S;
function reset(){var g=1;S={t:0,seed:0x9e3779b9,ranges:[],sealed:[false],cols:[],nextSeg:1};S.ranges.push({lo:0,size:K,seg:S.nextSeg++,age:0});S.sealed.push(false)}
function rnd(){var a=(S.seed=(S.seed+0x6D2B79F5)|0);a=Math.imul(a^(a>>>15),a|1);a^=a+Math.imul(a^(a>>>7),a|61);return((a^(a>>>14))>>>0)/4294967296}
function demand(t){var ph=(t%PERIOD)/PERIOD,x=ph<0.5?ph*2:2-ph*2;return 3+62*x*x*(3-2*x)}
function newSeg(){S.sealed.push(false);return S.nextSeg++}
function seal(rg,fence){S.sealed[rg.seg]=true;fence.push([rg.lo,rg.size])}
var keep=400;
function step(){
S.t++;var D=demand(S.t),col={cells:new Int32Array(K),fence:[],bounds:[]},next=[],i,rg;
for(i=0;i<S.ranges.length;i++){rg=S.ranges[i];var share=D*wsum(rg.lo,rg.size)/WT;
if(share>9.5&&rg.size>MIN){seal(rg,col.fence);var h=rg.size/2;next.push({lo:rg.lo,size:h,seg:newSeg(),age:0},{lo:rg.lo+h,size:h,seg:newSeg(),age:0})}
else next.push(rg)}
S.ranges=next;
for(i=0;i<S.ranges.length-1;i++){var a=S.ranges[i],b=S.ranges[i+1];
if(a.size===b.size&&a.size<K&&a.lo%(a.size*2)===0&&b.lo===a.lo+a.size&&D*wsum(a.lo,a.size*2)/WT<3.6&&a.age>8&&b.age>8){seal(a,col.fence);seal(b,col.fence);S.ranges.splice(i,2,{lo:a.lo,size:a.size*2,seg:newSeg(),age:0});break}}
for(i=0;i<S.ranges.length;i++){rg=S.ranges[i];rg.age++;if(rg.age>=SEGMAX){seal(rg,col.fence);rg.seg=newSeg();rg.age=0}
if(rg.lo>0)col.bounds.push(rg.lo);
for(var k=rg.lo;k<rg.lo+rg.size;k++){var p=Math.min(0.8,0.04+D*W[k]/WT*0.42);if(rnd()<p)col.cells[k]=rg.seg}}
S.cols.push(col);if(S.cols.length>keep)S.cols.splice(0,S.cols.length-keep)}
var dpr=1,cw=0,ch=0,stepX=9,rowH=9,padY=10,ncols=0;
function layout(){var b=cv.getBoundingClientRect();dpr=Math.min(2,window.devicePixelRatio||1);cw=b.width;ch=b.height;cv.width=Math.round(cw*dpr);cv.height=Math.round(ch*dpr);ctx.setTransform(dpr,0,0,dpr,0,0);padY=Math.max(8,ch*0.04);rowH=(ch-padY*2)/K;stepX=cw<560?7:9;ncols=Math.ceil(cw/stepX)+2;keep=Math.max(ncols+4,60)}
function draw(frac){
ctx.clearRect(0,0,cw,ch);var n=S.cols.length,first=Math.max(0,n-ncols),right=cw-stepX*0.5,dot=cw<560?1.7:2.1;
var empt=new Path2D(),act=new Path2D(),sea=new Path2D(),lin=new Path2D();
for(var i=first;i<n;i++){var col=S.cols[i],x=right-(n-1-i+frac)*stepX;if(x<-stepX)continue;
for(var k=0;k<K;k++){var y=padY+(k+0.5)*rowH,id=col.cells[k];
if(!id)empt.rect(x-0.6,y-0.6,1.2,1.2);
else if(S.sealed[id])sea.rect(x-dot*0.85,y-dot*0.85,dot*1.7,dot*1.7);
else{act.moveTo(x+dot,y);act.arc(x,y,dot,0,Math.PI*2)}}
for(var b=0;b<col.bounds.length;b++){var yb=padY+col.bounds[b]*rowH;lin.rect(x-0.7,yb-0.7,1.4,1.4)}
for(var f=0;f<col.fence.length;f++){var fx=x-stepX*0.5,lo=col.fence[f][0],sz=col.fence[f][1];for(var yy=padY+lo*rowH+1;yy<padY+(lo+sz)*rowH;yy+=3)lin.rect(fx-0.7,yy,1.4,1.4)}}
ctx.fillStyle=C["--border"];ctx.fill(empt);ctx.fillStyle=C["--state-sealed"];ctx.fill(sea);ctx.fillStyle=C["--state-active"];ctx.fill(act);ctx.fillStyle=C["--muted-foreground"];ctx.fill(lin)}
var running=false,paused=false,visible=true,raf=0,last=0,acc=0,lastDraw=0,hp=pausable(cv.parentNode,function(p){paused=p;sync()});
function frame(now){raf=0;if(!running)return;if(!last)last=now;acc+=now-last;last=now;while(acc>=TICK){step();acc-=TICK}
if(now-lastDraw>=1000/FPS-1){draw(acc/TICK);lastDraw=now}raf=requestAnimationFrame(frame)}
function sync(){var should=!mqRM.matches&&!paused&&visible&&!document.hidden;if(should&&!running){running=true;last=0;raf=requestAnimationFrame(frame)}else if(!should&&running){running=false;if(raf)cancelAnimationFrame(raf);raf=0;draw(acc/TICK)}}
function mode(){if(raf)cancelAnimationFrame(raf);raf=0;running=false;acc=0;layout();reset();
if(mqRM.matches){while(S.t<STATIC_T)step();hp.enable(false);draw(0)}
else{while(S.t<ncols)step();hp.enable(true);draw(0);sync()}}
colors();mode();
mqRM.addEventListener("change",mode);
document.addEventListener("visibilitychange",sync);
new MutationObserver(function(){colors();draw(running?acc/TICK:0)}).observe(root,{attributes:true,attributeFilter:["class"]});
if("IntersectionObserver" in window)new IntersectionObserver(function(es){visible=es[0].isIntersecting;sync()}).observe(cv);
var rt=0;new ResizeObserver(function(){clearTimeout(rt);rt=setTimeout(function(){var had=S.cols.length;layout();if(mqRM.matches||had<ncols){mode()}else draw(acc/TICK)},120)}).observe(cv);
}
function init(){applyTheme();tabs();thesis()}
if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",init);else init();
})();
