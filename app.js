/* ===========================
   LOCAL STORAGE
=========================== */
const LS_KEY = "planner_prototype_ua_v2_full";
const THEME_KEY = "planner_theme_pref";
const SYNC_URL = "/sync";
const SYNC_POLL_MS = 30000;
const SYNC_DEBOUNCE_MS = 2500;
const DEVICE_ID_KEY = "planner_device_id";
let _syncTimer = null;
let _syncInFlight = false;
let _lastPullAt = null;
let _lastPushAt = null;
let _overdueTimer = null;
let _syncReady = false;
let _syncPending = false;
let _syncInitDone = !SYNC_URL;
const memoryStorage = {};
function safeGet(key){
  try{
    return localStorage.getItem(key);
  } catch{
    return Object.prototype.hasOwnProperty.call(memoryStorage, key) ? memoryStorage[key] : null;
  }
}
function safeSet(key, value){
  try{
    localStorage.setItem(key, value);
  } catch{
    memoryStorage[key] = String(value);
  }
}
function safeRemove(key){
  try{
    localStorage.removeItem(key);
  } catch{
    delete memoryStorage[key];
  }
}
let _deviceId = null;
function getDeviceId(){
  if(_deviceId) return _deviceId;
  let id = safeGet(DEVICE_ID_KEY);
  if(!id){
    id = `dev_${Math.random().toString(16).slice(2)}_${Date.now().toString(16)}`;
    safeSet(DEVICE_ID_KEY, id);
  }
  _deviceId = id;
  return id;
}

function kyivNow(){
  const d = new Date();
  const parts = new Intl.DateTimeFormat('uk-UA', {
    timeZone: 'Europe/Kyiv',
    year:'numeric', month:'2-digit', day:'2-digit',
    hour:'2-digit', minute:'2-digit', second:'2-digit',
    hour12:false
  }).formatToParts(d).reduce((acc,p)=>{acc[p.type]=p.value; return acc;},{});
  const iso = `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}`;
  return new Date(iso);
}
function kyivDateStr(dateObj=kyivNow()){
  const y = dateObj.getFullYear();
  const m = String(dateObj.getMonth()+1).padStart(2,'0');
  const d = String(dateObj.getDate()).padStart(2,'0');
  return `${y}-${m}-${d}`;
}
function isWeekend(dateObj=kyivNow()){
  const day = dateObj.getDay(); // 0 Sun .. 6 Sat
  return day === 0 || day === 6;
}
function minutesSinceMidnight(dateObj=kyivNow()){
  return dateObj.getHours()*60 + dateObj.getMinutes();
}
const REPORT_DEADLINE_MIN = 17*60 + 30;

function uid(prefix="id"){
  return `${prefix}_${Math.random().toString(16).slice(2)}_${Date.now().toString(16)}`;
}
function migrateState(st){
  if(!st || typeof st !== "object") return null;

  const rawTasks = Array.isArray(st.tasks) ? st.tasks : [];
  const tasks = rawTasks.map(t=>{
    if(!t || typeof t !== "object") return t;
    const task = {...t};
    task.controlAlways = !!task.controlAlways;
    if(task.dueDate){
      task.nextControlDate = null;
      task.controlAlways = false;
    } else if(task.controlAlways){
      task.nextControlDate = null;
    }
    return task;
  });

  const next = {
    version: st.version ?? 0,
    session: st.session ?? { userId: null },
    departments: Array.isArray(st.departments) ? st.departments : [],
    users: Array.isArray(st.users) ? st.users : [],
    delegations: Array.isArray(st.delegations) ? st.delegations : [],
    tasks,
    taskUpdates: Array.isArray(st.taskUpdates) ? st.taskUpdates : [],
    dailyReports: Array.isArray(st.dailyReports) ? st.dailyReports : [],
    deptSummaries: Array.isArray(st.deptSummaries) ? st.deptSummaries : [],
    sync: (st.sync && typeof st.sync === "object") ? st.sync : null,
  };

  if(next.version < 4){
    next.version = 4;
  }
  if(Array.isArray(next.departments) && next.departments.length){
    const deptMap = {
      "Відділ №1":"Відділ БАС",
      "Відділ №2":"Відділ НРК",
      "Відділ №3":"Відділ МБеС",
      "Відділ №4":"Відділ БС",
      "Відділ №5":"Відділ ІОЗ",
      "Відділ №6":"Відділ КПЗБС",
      "Відділ №7":"Відділ РТБС",
      "Відділ 1":"Відділ БАС",
      "Відділ 2":"Відділ НРК",
      "Відділ 3":"Відділ МБеС",
      "Відділ 4":"Відділ БС",
      "Відділ 5":"Відділ ІОЗ",
      "Відділ 6":"Відділ КПЗБС",
      "Відділ 7":"Відділ РТБС",
      "Відділ № 1":"Відділ БАС",
      "Відділ № 2":"Відділ НРК",
      "Відділ № 3":"Відділ МБеС",
      "Відділ № 4":"Відділ БС",
      "Відділ № 5":"Відділ ІОЗ",
      "Відділ № 6":"Відділ КПЗБС",
      "Відділ № 7":"Відділ РТБС",
    };
    next.departments = next.departments.map(d=>{
      if(!d || !d.name) return d;
      const mapped = deptMap[d.name];
      return mapped ? {...d, name: mapped} : d;
    });
  }

  return next;
}
function loadState(){
  const raw = safeGet(LS_KEY);
  if(!raw) return null;
  try{
    const parsed = JSON.parse(raw);
    return migrateState(parsed);
  } catch{
    return null;
  }
}
function ensureSyncMeta(st){
  if(!st.sync || typeof st.sync !== "object") st.sync = {};
  if(!st.sync.deviceId) st.sync.deviceId = getDeviceId();
  if(typeof st.sync.revision !== "number") st.sync.revision = 0;
}
function stateForSync(st){
  return {...st, session: {userId: null}};
}
function markStateChanged(st){
  ensureSyncMeta(st);
  st.sync.updatedAt = nowIsoKyiv();
  st.sync.revision += 1;
}
function saveState(st, opts={}){
  if(!opts.skipSyncStamp){
    markStateChanged(st);
    queueSync();
  }
  safeSet(LS_KEY, JSON.stringify(st));
}
function nowIsoKyiv(){
  const d = kyivNow();
  const y = d.getFullYear();
  const m = String(d.getMonth()+1).padStart(2,'0');
  const da = String(d.getDate()).padStart(2,'0');
  const hh = String(d.getHours()).padStart(2,'0');
  const mm = String(d.getMinutes()).padStart(2,'0');
  const ss = String(d.getSeconds()).padStart(2,'0');
  return `${y}-${m}-${da} ${hh}:${mm}:${ss}`;
}
function addDays(dateStr, days){
  const [y,m,d] = dateStr.split('-').map(Number);
  const dt = new Date(y, m-1, d);
  dt.setDate(dt.getDate()+days);
  const yy = dt.getFullYear();
  const mm = String(dt.getMonth()+1).padStart(2,'0');
  const dd = String(dt.getDate()).padStart(2,'0');
  return `${yy}-${mm}-${dd}`;
}

function seed(){
  const today = kyivDateStr();
  const st = {
    version: 4,
    session: { userId: null },
    departments: [
      {id:"d1", name:"Відділ БАС"},
      {id:"d2", name:"Відділ НРК"},
      {id:"d3", name:"Відділ МБеС"},
      {id:"d4", name:"Відділ БС"},
      {id:"d5", name:"Відділ ІОЗ"},
      {id:"d6", name:"Відділ КПЗБС"},
      {id:"d7", name:"Відділ РТБС"},
    ],
    users: [
      {id:"u_boss", login:"boss", pass:"1234", name:"Керівник", role:"boss", departmentId:null, active:true},
      {id:"u_h2", login:"head2", pass:"1234", name:"Начальник Відділу №2", role:"dept_head", departmentId:"d2", active:true},
      {id:"u_h5", login:"head5", pass:"1234", name:"Начальник Відділу №5", role:"dept_head", departmentId:"d5", active:true},
      {id:"u_e21", login:"e21", pass:"1234", name:"Виконавець 2-1", role:"executor", departmentId:"d2", active:true},
      {id:"u_e22", login:"e22", pass:"1234", name:"Виконавець 2-2", role:"executor", departmentId:"d2", active:true},
      {id:"u_e51", login:"e51", pass:"1234", name:"Виконавець 5-1", role:"executor", departmentId:"d5", active:true},
      {id:"u_e41", login:"e41", pass:"1234", name:"Виконавець 4-1", role:"executor", departmentId:"d4", active:true},
    ],
    delegations: [],
    tasks: [],
    taskUpdates: [],
    dailyReports: [],
    deptSummaries: []
  };
  saveState(st);
  return st;
}

let STATE = loadState() || seed();

/* ===========================
   GETTERS / HELPERS
=========================== */
function getUserById(id){ return STATE.users.find(u=>u.id===id) || null; }
function getDeptById(id){ return STATE.departments.find(d=>d.id===id) || null; }
function currentSessionUser(){ return STATE.session.userId ? getUserById(STATE.session.userId) : null; }

function htmlesc(s){
  return (s ?? "").toString()
    .replaceAll("&","&amp;").replaceAll("<","&lt;")
    .replaceAll(">","&gt;").replaceAll('"',"&quot;")
    .replaceAll("'","&#039;");
}
function fmtDate(d){
  if(!d) return "—";
  const [y,m,da] = d.split("T")[0].split("-");
  return `${da}.${m}.${y}`;
}
function fmtDateShort(d){
  if(!d) return "—";
  const parts = fmtDate(d).split(".");
  return `${parts[0]}.${parts[1]}`;
}
function splitDateTime(v){
  if(!v) return {date:"", time:""};
  const [date, time] = v.split("T");
  return {date, time: time ? time.slice(0,5) : ""};
}
function joinDateTime(date, time){
  if(!date) return null;
  if(!time) return date;
  return `${date}T${time}`;
}
function dueDisplay(due){
  if(!due) return "—";
  const {date, time} = splitDateTime(due);
  if(!time) return fmtDateShort(date);
  const today = kyivDateStr();
  if(date === today) return time;
  return `${time} ${fmtDateShort(date)}`;
}
function dueTitle(due){
  if(!due) return "Без дедлайну";
  const {date, time} = splitDateTime(due);
  return time ? `${fmtDate(date)} ${time}` : fmtDate(date);
}
function dueSortKey(due){
  if(!due) return "9999-99-99T99:99";
  const {date, time} = splitDateTime(due);
  return `${date}T${time || "00:00"}`;
}
function splitDateTimeLoose(v){
  if(!v) return {date:"", time:""};
  const str = String(v).trim();
  if(str.includes("T")){
    const [date, time] = str.split("T");
    return {date, time: time ? time.slice(0,5) : ""};
  }
  if(str.includes(" ")){
    const [date, time] = str.split(" ");
    return {date, time: time ? time.slice(0,5) : ""};
  }
  if(/^\d{4}-\d{2}-\d{2}$/.test(str)) return {date: str, time:""};
  return {date:"", time:""};
}
function closeDisplay(dt){
  const {date, time} = splitDateTimeLoose(dt);
  if(!date) return "—";
  const t = time ? time.replace(":",".") : "";
  return t ? `${fmtDateShort(date)} ${t}` : fmtDateShort(date);
}
function closeTitle(dt){
  const {date, time} = splitDateTimeLoose(dt);
  if(!date) return "—";
  return time ? `${fmtDate(date)} ${time}` : `${fmtDate(date)}`;
}
function deptShortLabel(dept){
  if(!dept?.name) return "Особ.";
  if(dept.name.startsWith("Відділ ")){
    return dept.name.replace(/^Відділ\s+/,"").trim();
  }
  const m = dept.name.match(/№\s*\d+/);
  return m ? m[0].replace(/\s+/g,"") : dept.name;
}
function deptBadgeHtml(dept){
  const name = dept?.name || "Відділ";
  const short = deptShortLabel(dept);
  return `<span class="dept-badge" title="${htmlesc(name)}">${htmlesc(short)}</span>`;
}
function getDeptResponsibleOptions(deptId){
  return STATE.users.filter(x=>x.active && x.departmentId===deptId && (x.role==="executor" || x.role==="dept_head"));
}
function canEditTask(u, t){
  if(!u || !t) return false;
  if(u.role==="boss") return true;
  const {isDeptHeadLike} = asDeptRole(u);
  if(!isDeptHeadLike) return false;
  if(t.type==="personal" || t.type==="managerial") return false;
  return t.departmentId === u.departmentId;
}
function shorten(s, max=70){
  s = (s || "").trim();
  if(!s) return "—";
  return s.length>max ? s.slice(0,max-1)+"…" : s;
}

/* ===========================
   DELEGATIONS (в.о.)
=========================== */
function recomputeDelegationStatuses(){
  const today = kyivDateStr();
  STATE.delegations = STATE.delegations.map(d=>{
    if(d.status==="скасовано") return d;
    if(d.startDate > today) return {...d, status:"заплановано"};
    if(d.untilCancel) return {...d, status:"активне"};
    if(d.endDate && today <= d.endDate) return {...d, status:"активне"};
    return {...d, status:"завершено"};
  });
}
function activeDelegationForDept(deptId, dateStr=kyivDateStr()){
  const list = STATE.delegations.filter(x=>x.departmentId===deptId);
  const today = dateStr;
  return list.find(x=>{
    if(x.status==="скасовано" || x.status==="завершено") return false;
    const starts = x.startDate <= today;
    if(!starts) return false;
    if(x.untilCancel) return true;
    return today <= x.endDate;
  }) || null;
}
function effectiveDeptHeadUserId(deptId){
  const del = activeDelegationForDept(deptId);
  if(del) return del.actingHeadUserId;
  const head = STATE.users.find(u=>u.role==="dept_head" && u.departmentId===deptId && u.active);
  return head ? head.id : null;
}
function isActingHead(userId){
  const today = kyivDateStr();
  return STATE.delegations.some(d=>{
    if(d.status==="скасовано" || d.status==="завершено") return false;
    if(d.actingHeadUserId !== userId) return false;
    if(d.startDate > today) return false;
    if(d.untilCancel) return true;
    return today <= d.endDate;
  });
}
function actingBannerForUser(u){
  if(!u || u.role==="boss") return null;
  const today = kyivDateStr();
  const del = STATE.delegations.find(d=>{
    if(d.status==="скасовано" || d.status==="завершено") return false;
    if(d.actingHeadUserId !== u.id) return false;
    if(d.startDate > today) return false;
    if(d.untilCancel) return true;
    return today <= d.endDate;
  });
  if(!del) return null;
  const dept = getDeptById(del.departmentId);
  const until = del.untilCancel ? "до скасування" : `до ${del.endDate}`;
  return `🟦 Ви в.о. начальника ${dept?.name ?? "відділу"} ${until}`;
}

/* ===========================
   PERMISSIONS
=========================== */
function canAccessDept(u, deptId){
  if(!u) return false;
  if(u.role==="boss") return true;
  return u.departmentId === deptId;
}
function asDeptRole(u){
  if(!u || u.role==="boss") return {scopeDeptId:null, isDeptHeadLike:false};
  const deptId = u.departmentId;
  const eff = effectiveDeptHeadUserId(deptId);
  return {scopeDeptId:deptId, isDeptHeadLike: (eff === u.id)};
}

/* ===========================
   TASK LOGIC
=========================== */
function statusLabel(s){
  const map = {
    "на_контролі":"На контролі",
    "в_процесі":"В процесі",
    "очікування":"Очікування",
    "блокер":"Блокер",
    "очікує_підтвердження":"Очікує підтвердження",
    "закрито":"Закрито",
    "скасовано":"Скасовано",
  };
  return map[s] || s;
}
function statusIcon(s){
  const map = {
    "на_контролі":"🧭",
    "в_процесі":"🔄",
    "очікування":"⏳",
    "блокер":"⛔",
    "очікує_підтвердження":"🟣",
    "закрито":"✅",
    "скасовано":"✖️",
  };
  return map[s] || "•";
}
function statusBadgeClass(s){
  if(s==="закрито") return "b-ok";
  if(s==="блокер" || s==="очікування") return "b-warn";
  if(s==="очікує_підтвердження") return "b-violet";
  if(s==="в_процесі" || s==="на_контролі") return "b-blue";
  return "";
}
function priorityIcon(p){
  const map = {
    "терміново":"Т",
    "високий":"В",
    "звичайний":"З",
    "низький":"Н",
  };
  return map[p] || "•";
}
function controlMeta(task){
  if(task.dueDate){
    return {label:"", title:"", exportValue:""};
  }
  if(task.controlAlways){
    return {label:"постійно", title:"Контроль: постійно", exportValue:"постійно"};
  }
  if(task.nextControlDate){
    return {
      label: fmtDateShort(task.nextControlDate),
      title: `Контроль ${fmtDate(task.nextControlDate)}`,
      exportValue: task.nextControlDate
    };
  }
  return {label:"", title:"", exportValue:""};
}
function controlSortKey(task){
  if(task.dueDate) return "9999-99-99";
  if(task.controlAlways) return "0000-00-00";
  return task.nextControlDate || "9999-99-99";
}
function controlHint(task){
  if(task.controlAlways) return "Контроль: постійно.";
  if(task.nextControlDate) return `Контроль на ${fmtDate(task.nextControlDate)}.`;
  return "";
}
function lastBlockerUpdate(task){
  return STATE.taskUpdates
    .filter(u=>u.taskId===task.id && (u.status==="блокер" || u.status==="очікування"))
    .sort((a,b)=>b.at.localeCompare(a.at))[0] || null;
}
function getCloseUpdate(task){
  if(!task) return null;
  const upd = STATE.taskUpdates
    .filter(u=>u.taskId===task.id && u.status==="закрито")
    .sort((a,b)=>b.at.localeCompare(a.at))[0];
  if(upd) return upd;
  if(task.status==="закрито") return {at: task.updatedAt || "", note: ""};
  return null;
}
function normalizeCloseNote(note){
  if(!note) return "";
  return String(note)
    .replace(/^Статус\s*→\s*[^:]+:\s*/i, "")
    .replace(/^Розблоковано\s*→\s*[^:]+:\s*/i, "")
    .replace(/^Закрито:\s*/i, "")
    .trim();
}
function isOverdue(task){
  if(!task?.dueDate) return false;
  if(task.status === "закрито" || task.status === "скасовано") return false;
  const today = kyivDateStr();
  const {date, time} = splitDateTime(task.dueDate);
  if(!date) return false;
  if(date < today) return true;
  if(date > today) return false;
  if(!time) return false;
  const now = kyivNow();
  const nowMin = now.getHours()*60 + now.getMinutes();
  const [hh, mm] = time.split(":").map(Number);
  const dueMin = (hh || 0)*60 + (mm || 0);
  return nowMin >= dueMin;
}
function isDueToday(task){
  if(!task?.dueDate) return false;
  const {date} = splitDateTime(task.dueDate);
  if(!date) return false;
  return date === kyivDateStr();
}
function needsControl(task){
  const today = kyivDateStr();
  if(task.controlAlways) return true;
  if(!task.nextControlDate) return false;
  if(task.status === "закрито" || task.status === "скасовано") return false;
  return task.nextControlDate <= today;
}
function staleTask(task, days=7){
  const updates = STATE.taskUpdates.filter(u=>u.taskId===task.id).sort((a,b)=>a.at.localeCompare(b.at));
  const lastAt = updates.length ? updates[updates.length-1].at : task.updatedAt;
  const lastDate = lastAt.slice(0,10);
  const today = kyivDateStr();
  const diff = dateDiffDays(lastDate, today);
  return diff > days && task.status !== "закрито" && task.status !== "скасовано";
}
function dateDiffDays(a,b){
  const [ay,am,ad] = a.split("-").map(Number);
  const [by,bm,bd] = b.split("-").map(Number);
  const da = new Date(ay,am-1,ad);
  const db = new Date(by,bm-1,bd);
  return Math.round((db-da)/(1000*60*60*24));
}
function getVisibleTasksForUser(u){
  if(!u) return [];
  if(u.role==="boss") return STATE.tasks.filter(t=>!isAnnouncement(t));
  return STATE.tasks.filter(t=>!isAnnouncement(t) && t.departmentId===u.departmentId);
}
function updateTask(taskId, patch, authorId, note){
  const idx = STATE.tasks.findIndex(t=>t.id===taskId);
  if(idx < 0) return;
  STATE.tasks[idx] = {...STATE.tasks[idx], ...patch, updatedAt: nowIsoKyiv()};
  STATE.taskUpdates.push({
    id: uid("upd"),
    taskId,
    authorUserId: authorId,
    at: nowIsoKyiv(),
    note: note || "",
    status: patch.status || STATE.tasks[idx].status
  });
  saveState(STATE);
}
function createTask(task, authorId){
  STATE.tasks.push(task);
  const note = task?.category === "announcement" ? "Створено оголошення" : "Створено задачу";
  STATE.taskUpdates.push({
    id: uid("upd"),
    taskId: task.id,
    authorUserId: authorId,
    at: nowIsoKyiv(),
    note,
    status: task.status
  });
  saveState(STATE);
}
function genTaskCode(prefix){
  const year = kyivDateStr().slice(0,4);
  const nums = STATE.tasks
    .filter(t=>t.id.startsWith(prefix+"-"+year))
    .map(t=>Number(t.id.split("-").pop()))
    .filter(n=>Number.isFinite(n));
  const next = (nums.length ? Math.max(...nums) : 0) + 1;
  return `${prefix}-${year}-${String(next).padStart(4,'0')}`;
}
function normalizeSheetName(name){
  return (name || "Sheet")
    .replace(/[\\\/\?\*\[\]\:]/g, " ")
    .trim()
    .slice(0,31) || "Sheet";
}
function xmlEsc(s){
  return (s ?? "").toString()
    .replaceAll("&","&amp;")
    .replaceAll("<","&lt;")
    .replaceAll(">","&gt;")
    .replaceAll('"',"&quot;")
    .replaceAll("'","&apos;");
}
function toDateOnly(v){
  if(!v) return null;
  const str = String(v);
  if(/^\d{4}-\d{2}-\d{2}$/.test(str)) return str;
  const m = str.match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}
function inRange(dateStr, from, to){
  if(!dateStr) return false;
  return dateStr >= from && dateStr <= to;
}
function taskInPeriod(task, from, to){
  const check = [
    toDateOnly(task.createdAt),
    toDateOnly(task.updatedAt),
    toDateOnly(task.startDate),
  ].filter(Boolean);
  return check.some(d=>inRange(d, from, to));
}
function taskTypeLabel(type){
  if(type==="managerial") return "Управлінська";
  if(type==="internal") return "Внутрішня";
  if(type==="personal") return "Моя задача";
  return type;
}
function isAnnouncement(t){
  return !!t && t.category === "announcement";
}
function announcementAudienceLabel(a){
  if(a === "meeting") return "Нарада";
  return "Особовий склад";
}
function canSeeAnnouncement(u, t){
  if(!u || !isAnnouncement(t)) return false;
  if(u.role === "boss") return true;
  if(t.audience === "staff") return true;
  if(t.audience === "meeting"){
    const {isDeptHeadLike} = asDeptRole(u);
    return !!isDeptHeadLike;
  }
  return false;
}
function getVisibleAnnouncementsForUser(u){
  if(!u) return [];
  return STATE.tasks.filter(isAnnouncement).filter(t=>canSeeAnnouncement(u, t));
}
function taskExportRows(tasks){
  return tasks.map(t=>{
    const dept = t.departmentId ? getDeptById(t.departmentId)?.name : "Особисто";
    const resp = getUserById(t.responsibleUserId)?.name || "";
    const creator = getUserById(t.createdBy)?.name || t.createdBy || "";
    const ctrl = controlMeta(t);
    return [
      t.id,
      t.title,
      taskTypeLabel(t.type),
      statusLabel(t.status),
      dept || "",
      resp,
      t.priority || "",
      t.startDate || "",
      t.dueDate || "",
      ctrl.exportValue || "",
      toDateOnly(t.updatedAt) || "",
      creator,
    ];
  });
}
function sortedTasksForExport(tasks){
  return tasks.slice().sort((a,b)=>{
    const bucket = (t)=>{
      if(t.dueDate) return 0;
      if(["блокер","очікування"].includes(t.status)) return 1;
      if(t.nextControlDate) return 2;
      if(t.controlAlways) return 3;
      return 4;
    };
    const dateKey = (t)=>{
      if(t.dueDate) return dueSortKey(t.dueDate);
      if(t.nextControlDate) return t.nextControlDate;
      if(t.controlAlways) return "0000-00-00";
      return "9999-99-99";
    };
    const ba = bucket(a);
    const bb = bucket(b);
    if(ba!==bb) return ba - bb;
    const dka = dateKey(a);
    const dkb = dateKey(b);
    if(dka!==dkb) return dka.localeCompare(dkb);
    return (a.title || "").localeCompare(b.title || "");
  });
}
function lastUpdateByTask(tasks){
  const ids = new Set(tasks.map(t=>t.id));
  const map = {};
  STATE.taskUpdates.forEach(u=>{
    if(!ids.has(u.taskId)) return;
    if(!map[u.taskId] || (map[u.taskId].at || "") < (u.at || "")){
      map[u.taskId] = u;
    }
  });
  return map;
}
function taskExportRowsFull(tasks){
  const sorted = sortedTasksForExport(tasks);
  const lastMap = lastUpdateByTask(sorted);
  return sorted.map((t, idx)=>{
    const resp = getUserById(t.responsibleUserId)?.name || "";
    const creator = getUserById(t.createdBy)?.name || t.createdBy || "";
    const ctrl = controlMeta(t);
    const last = lastMap[t.id];
    const lastAuthor = last ? (getUserById(last.authorUserId)?.name || last.authorUserId || "") : "";
    const lastText = last
      ? `${toDateOnly(last.at) || ""} ${lastAuthor}: ${shorten(last.note || statusLabel(last.status) || "", 80)}`
      : "";
    const updates = STATE.taskUpdates
      .filter(u=>u.taskId===t.id)
      .sort((a,b)=>(a.at || "").localeCompare(b.at || ""))
      .map(u=>{
        const au = getUserById(u.authorUserId)?.name || u.authorUserId || "";
        const note = u.note || statusLabel(u.status) || "";
        const d = toDateOnly(u.at) || "";
        return `${d} ${au}: ${note}`;
      }).join(" | ");
    const updatesShort = shorten(updates, 200);
    return [
      `${idx+1}.`,
      t.id,
      t.title,
      taskTypeLabel(t.type),
      statusLabel(t.status),
      t.startDate || "",
      t.dueDate || "",
      ctrl.exportValue || "",
      t.priority || "",
      resp,
      toDateOnly(t.updatedAt) || "",
      updatesShort || lastText,
      creator,
    ];
  });
}
function autoCols(data, min=8, max=40){
  if(!data.length) return [];
  return data[0].map((_, i)=>{
    let w = min;
    data.forEach(row=>{
      const v = row[i];
      const len = (v === null || v === undefined) ? 0 : String(v).length;
      if(len > w) w = len;
    });
    return {wch: Math.min(max, Math.max(min, w + 2))};
  });
}
function buildWorksheetXmlRaw(name, rows){
  const rowsXml = rows.map(r=>`<Row>${r.map(v=>`<Cell><Data ss:Type="String">${xmlEsc(v)}</Data></Cell>`).join("")}</Row>`).join("");
  return `<Worksheet ss:Name="${xmlEsc(normalizeSheetName(name))}"><Table>${rowsXml}</Table></Worksheet>`;
}
function buildAnalyticsRows(){
  const today = kyivDateStr();
  const days = Array.from({length:7}, (_,i)=>addDays(today, -(6-i)));
  const closeDateForTask = (task)=>{
    const updates = STATE.taskUpdates
      .filter(u=>u.taskId===task.id && u.status==="закрито")
      .sort((a,b)=>b.at.localeCompare(a.at));
    if(updates[0]) return toDateOnly(updates[0].at);
    if(task.status==="закрито") return toDateOnly(task.updatedAt);
    return null;
  };
  const weekClosed = days.map(d=>{
    const count = STATE.tasks.filter(t=>closeDateForTask(t)===d).length;
    return {date:d, count};
  });
  const closedDurations = STATE.tasks
    .map(t=>{
      const closeDate = closeDateForTask(t);
      const startDate = toDateOnly(t.createdAt) || t.startDate;
      if(!closeDate || !startDate) return null;
      const daysToClose = dateDiffDays(startDate, closeDate);
      if(daysToClose < 0) return null;
      return {task:t, daysToClose};
    })
    .filter(Boolean);
  const avgClose = closedDurations.length
    ? (closedDurations.reduce((s,x)=>s+x.daysToClose, 0) / closedDurations.length).toFixed(1)
    : "—";
  const topProblems = STATE.tasks
    .map(t=>{
      const blockerUpdates = STATE.taskUpdates.filter(u=>u.taskId===t.id && (u.status==="блокер" || u.status==="очікування"));
      return {task:t, count:blockerUpdates.length, last:blockerUpdates.sort((a,b)=>b.at.localeCompare(a.at))[0]};
    })
    .filter(x=>x.count>0)
    .sort((a,b)=>b.count-a.count)
    .slice(0,5);
  const deptLoad = STATE.departments.map(d=>{
    const deptTasks = STATE.tasks.filter(t=>t.departmentId===d.id);
    const active = deptTasks.filter(t=>t.status!=="закрито" && t.status!=="скасовано").length;
    const blockers = deptTasks.filter(t=>t.status==="блокер" || t.status==="очікування").length;
    const overdue = deptTasks.filter(t=>isOverdue(t)).length;
    return {dept:d, active, blockers, overdue};
  });
  const activeDeptTasks = STATE.tasks.filter(t=>t.departmentId && t.status!=="закрито" && t.status!=="скасовано");
  const recentClosed = STATE.tasks.filter(t=>t.departmentId && t.status==="закрито" && closeDateForTask(t) && closeDateForTask(t) >= days[0] && closeDateForTask(t) <= days[days.length-1]);
  const priorityKeys = ["терміново","високий","звичайний","низький"];
  const priorityLabels = {
    "терміново":"Терміново",
    "високий":"Високий",
    "звичайний":"Звичайний",
    "низький":"Низький"
  };
  const priorityCounts = priorityKeys.map(k=>({
    key:k,
    label: priorityLabels[k] || k,
    count: activeDeptTasks.filter(t=>t.priority===k).length
  }));
  const priorityOther = activeDeptTasks.filter(t=>!priorityKeys.includes(t.priority)).length;
  if(priorityOther>0){
    priorityCounts.push({key:"other", label:"Без пріоритету", count: priorityOther});
  }
  const priorityClosed = priorityKeys.map(k=>({
    key:k,
    label: priorityLabels[k] || k,
    count: recentClosed.filter(t=>t.priority===k).length
  }));
  const priorityClosedOther = recentClosed.filter(t=>!priorityKeys.includes(t.priority)).length;
  if(priorityClosedOther>0){
    priorityClosed.push({key:"other", label:"Без пріоритету", count: priorityClosedOther});
  }
  const priorityBreakdown = (list)=>{
    const rows = priorityKeys.map(k=>({
      key:k,
      label: priorityLabels[k] || k,
      count: list.filter(t=>t.priority===k).length
    }));
    const other = list.filter(t=>!priorityKeys.includes(t.priority)).length;
    if(other>0){
      rows.push({key:"other", label:"Без пріоритету", count: other});
    }
    return {rows, total: list.length};
  };
  const activeDeadline = activeDeptTasks.filter(t=>!!t.dueDate);
  const activeCtrlDate = activeDeptTasks.filter(t=>!t.dueDate && !!t.nextControlDate && !t.controlAlways);
  const activeCtrlAlways = activeDeptTasks.filter(t=>!t.dueDate && !!t.controlAlways);
  const closedDeadline = recentClosed.filter(t=>!!t.dueDate);
  const closedCtrlDate = recentClosed.filter(t=>!t.dueDate && !!t.nextControlDate && !t.controlAlways);
  const closedCtrlAlways = recentClosed.filter(t=>!t.dueDate && !!t.controlAlways);
  const priActiveDeadline = priorityBreakdown(activeDeadline);
  const priActiveCtrlDate = priorityBreakdown(activeCtrlDate);
  const priActiveCtrlAlways = priorityBreakdown(activeCtrlAlways);
  const priClosedDeadline = priorityBreakdown(closedDeadline);
  const priClosedCtrlDate = priorityBreakdown(closedCtrlDate);
  const priClosedCtrlAlways = priorityBreakdown(closedCtrlAlways);

  const rows = [];
  rows.push([`АНАЛІТИКА (останні 7 днів)`]);
  rows.push([]);
  rows.push(["Графік закриття задач"]);
  rows.push(["Дата","Кількість"]);
  weekClosed.forEach(x=>rows.push([fmtDate(x.date), x.count]));
  rows.push([]);
  rows.push(["Середній час закриття (днів)", avgClose]);
  rows.push([]);
  rows.push(["Топ проблем"]);
  rows.push(["Задача","Відділ","К-сть блокерів","Останнє"]);
  topProblems.forEach(x=>{
    const dept = x.task.departmentId ? getDeptById(x.task.departmentId)?.name : "Особисто";
    const note = x.last?.note ? shorten(x.last.note, 80) : "";
    rows.push([x.task.title, dept || "", x.count, note]);
  });
  rows.push([]);
  rows.push(["Навантаження по відділах"]);
  rows.push(["Відділ","Активні","Блокери","Прострочені"]);
  deptLoad.forEach(x=>rows.push([x.dept.name, x.active, x.blockers, x.overdue]));
  rows.push([]);
  rows.push(["Пріоритети активних", activeDeptTasks.length]);
  rows.push(["Пріоритет","К-сть"]);
  priorityCounts.forEach(x=>rows.push([x.label, x.count]));
  rows.push([]);
  rows.push(["Активні з дедлайном — пріоритети", priActiveDeadline.total]);
  rows.push(["Пріоритет","К-сть"]);
  priActiveDeadline.rows.forEach(x=>rows.push([x.label, x.count]));
  rows.push([]);
  rows.push(["Активні з датою контролю — пріоритети", priActiveCtrlDate.total]);
  rows.push(["Пріоритет","К-сть"]);
  priActiveCtrlDate.rows.forEach(x=>rows.push([x.label, x.count]));
  rows.push([]);
  rows.push(["Активні на постійному контролі — пріоритети", priActiveCtrlAlways.total]);
  rows.push(["Пріоритет","К-сть"]);
  priActiveCtrlAlways.rows.forEach(x=>rows.push([x.label, x.count]));
  rows.push([]);
  rows.push(["Пріоритети закритих (7 днів)", recentClosed.length]);
  rows.push(["Пріоритет","К-сть"]);
  priorityClosed.forEach(x=>rows.push([x.label, x.count]));
  rows.push([]);
  rows.push(["Закриті з дедлайном — пріоритети (7 днів)", priClosedDeadline.total]);
  rows.push(["Пріоритет","К-сть"]);
  priClosedDeadline.rows.forEach(x=>rows.push([x.label, x.count]));
  rows.push([]);
  rows.push(["Закриті з датою контролю — пріоритети (7 днів)", priClosedCtrlDate.total]);
  rows.push(["Пріоритет","К-сть"]);
  priClosedCtrlDate.rows.forEach(x=>rows.push([x.label, x.count]));
  rows.push([]);
  rows.push(["Закриті на постійному контролі — пріоритети (7 днів)", priClosedCtrlAlways.total]);
  rows.push(["Пріоритет","К-сть"]);
  priClosedCtrlAlways.rows.forEach(x=>rows.push([x.label, x.count]));
  return rows;
}
function buildAnalyticsTableRows(){
  const today = kyivDateStr();
  const days = Array.from({length:7}, (_,i)=>addDays(today, -(6-i)));
  const closeDateForTask = (task)=>{
    const updates = STATE.taskUpdates
      .filter(u=>u.taskId===task.id && u.status==="закрито")
      .sort((a,b)=>b.at.localeCompare(a.at));
    if(updates[0]) return toDateOnly(updates[0].at);
    if(task.status==="закрито") return toDateOnly(task.updatedAt);
    return null;
  };
  const weekClosed = days.map(d=>{
    const count = STATE.tasks.filter(t=>closeDateForTask(t)===d).length;
    return {date:d, count};
  });
  const closedDurations = STATE.tasks
    .map(t=>{
      const closeDate = closeDateForTask(t);
      const startDate = toDateOnly(t.createdAt) || t.startDate;
      if(!closeDate || !startDate) return null;
      const daysToClose = dateDiffDays(startDate, closeDate);
      if(daysToClose < 0) return null;
      return {task:t, daysToClose};
    })
    .filter(Boolean);
  const avgClose = closedDurations.length
    ? (closedDurations.reduce((s,x)=>s+x.daysToClose, 0) / closedDurations.length).toFixed(1)
    : "—";

  const topProblems = STATE.tasks
    .map(t=>{
      const blockerUpdates = STATE.taskUpdates.filter(u=>u.taskId===t.id && (u.status==="блокер" || u.status==="очікування"));
      return {task:t, count:blockerUpdates.length};
    })
    .filter(x=>x.count>0)
    .sort((a,b)=>b.count-a.count)
    .slice(0,5);

  const deptLoad = STATE.departments.map(d=>{
    const deptTasks = STATE.tasks.filter(t=>t.departmentId===d.id);
    const active = deptTasks.filter(t=>t.status!=="закрито" && t.status!=="скасовано").length;
    const blockers = deptTasks.filter(t=>t.status==="блокер" || t.status==="очікування").length;
    const overdue = deptTasks.filter(t=>isOverdue(t)).length;
    return {dept:d, active, blockers, overdue};
  });

  const activeDeptTasks = STATE.tasks.filter(t=>t.departmentId && t.status!=="закрито" && t.status!=="скасовано");
  const recentClosed = STATE.tasks.filter(t=>t.departmentId && t.status==="закрито" && closeDateForTask(t) && closeDateForTask(t) >= days[0] && closeDateForTask(t) <= days[days.length-1]);
  const priorityKeys = ["терміново","високий","звичайний","низький"];
  const priorityLabels = {
    "терміново":"Терміново",
    "високий":"Високий",
    "звичайний":"Звичайний",
    "низький":"Низький"
  };
  const priorityCounts = priorityKeys.map(k=>({
    key:k,
    label: priorityLabels[k] || k,
    count: activeDeptTasks.filter(t=>t.priority===k).length
  }));
  const priorityOther = activeDeptTasks.filter(t=>!priorityKeys.includes(t.priority)).length;
  if(priorityOther>0){
    priorityCounts.push({key:"other", label:"Без пріоритету", count: priorityOther});
  }
  const priorityClosed = priorityKeys.map(k=>({
    key:k,
    label: priorityLabels[k] || k,
    count: recentClosed.filter(t=>t.priority===k).length
  }));
  const priorityClosedOther = recentClosed.filter(t=>!priorityKeys.includes(t.priority)).length;
  if(priorityClosedOther>0){
    priorityClosed.push({key:"other", label:"Без пріоритету", count: priorityClosedOther});
  }
  const priorityBreakdown = (list)=>{
    const rows = priorityKeys.map(k=>({
      key:k,
      label: priorityLabels[k] || k,
      count: list.filter(t=>t.priority===k).length
    }));
    const other = list.filter(t=>!priorityKeys.includes(t.priority)).length;
    if(other>0){
      rows.push({key:"other", label:"Без пріоритету", count: other});
    }
    return rows;
  };
  const activeDeadline = activeDeptTasks.filter(t=>!!t.dueDate);
  const activeCtrlDate = activeDeptTasks.filter(t=>!t.dueDate && !!t.nextControlDate && !t.controlAlways);
  const activeCtrlAlways = activeDeptTasks.filter(t=>!t.dueDate && !!t.controlAlways);
  const closedDeadline = recentClosed.filter(t=>!!t.dueDate);
  const closedCtrlDate = recentClosed.filter(t=>!t.dueDate && !!t.nextControlDate && !t.controlAlways);
  const closedCtrlAlways = recentClosed.filter(t=>!t.dueDate && !!t.controlAlways);

  const rows = [["Група","Сегмент","Показник","Значення"]];
  weekClosed.forEach(x=>rows.push(["Закриття","Дата", fmtDate(x.date), x.count]));
  rows.push(["Середній час закриття","Усереднено","Днів", avgClose]);
  topProblems.forEach(x=>rows.push(["Топ проблем","Задача", x.task.title, x.count]));
  deptLoad.forEach(x=>{
    rows.push(["Відділи", x.dept.name, "Активні", x.active]);
    rows.push(["Відділи", x.dept.name, "Блокери", x.blockers]);
    rows.push(["Відділи", x.dept.name, "Прострочені", x.overdue]);
  });
  priorityCounts.forEach(x=>rows.push(["Пріоритети (активні)","Всі", x.label, x.count]));
  priorityClosed.forEach(x=>rows.push(["Пріоритети (закриті 7 днів)","Всі", x.label, x.count]));
  priorityBreakdown(activeDeadline).forEach(x=>rows.push(["Активні з дедлайном","Пріоритет", x.label, x.count]));
  priorityBreakdown(activeCtrlDate).forEach(x=>rows.push(["Активні з датою контролю","Пріоритет", x.label, x.count]));
  priorityBreakdown(activeCtrlAlways).forEach(x=>rows.push(["Активні на постійному контролі","Пріоритет", x.label, x.count]));
  priorityBreakdown(closedDeadline).forEach(x=>rows.push(["Закриті з дедлайном (7 днів)","Пріоритет", x.label, x.count]));
  priorityBreakdown(closedCtrlDate).forEach(x=>rows.push(["Закриті з датою контролю (7 днів)","Пріоритет", x.label, x.count]));
  priorityBreakdown(closedCtrlAlways).forEach(x=>rows.push(["Закриті на постійному контролі (7 днів)","Пріоритет", x.label, x.count]));
  return rows;
}
function applyTimesFont(ws){
  if(!ws || !ws["!ref"]) return;
  const range = XLSX.utils.decode_range(ws["!ref"]);
  for(let R = range.s.r; R <= range.e.r; R++){
    for(let C = range.s.c; C <= range.e.c; C++){
      const cellRef = XLSX.utils.encode_cell({r:R,c:C});
      const cell = ws[cellRef];
      if(!cell) continue;
      cell.s = cell.s || {};
      cell.s.font = {name:"Times New Roman", sz:11, bold: R===0};
    }
  }
}
function buildWorksheetXml(name, header, rows){
  const headerXml = `<Row>${header.map(h=>`<Cell ss:StyleID="header"><Data ss:Type="String">${xmlEsc(h)}</Data></Cell>`).join("")}</Row>`;
  const rowsXml = rows.map(r=>`<Row>${r.map(v=>`<Cell><Data ss:Type="String">${xmlEsc(v)}</Data></Cell>`).join("")}</Row>`).join("");
  return `<Worksheet ss:Name="${xmlEsc(normalizeSheetName(name))}"><Table>${headerXml}${rowsXml}</Table></Worksheet>`;
}
function buildTasksWorkbookXml(sheets){
  return `<?xml version="1.0" encoding="UTF-8"?>
<?mso-application progid="Excel.Sheet"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
 xmlns:o="urn:schemas-microsoft-com:office:office"
 xmlns:x="urn:schemas-microsoft-com:office:excel"
 xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"
 xmlns:html="http://www.w3.org/TR/REC-html40">
 <Styles>
  <Style ss:ID="Default" ss:Name="Normal">
   <Alignment ss:Vertical="Bottom"/>
   <Borders/>
   <Font ss:FontName="Times New Roman" ss:Size="11"/>
   <Interior/>
   <NumberFormat/>
   <Protection/>
  </Style>
  <Style ss:ID="header">
   <Font ss:Bold="1" ss:FontName="Times New Roman"/>
   <Interior ss:Color="#DCE6F1" ss:Pattern="Solid"/>
  </Style>
 </Styles>
 ${sheets.join("\n")}
</Workbook>`;
}
function downloadExcelXml(filename, xml){
  const blob = new Blob(["\uFEFF"+xml], {type:"application/vnd.ms-excel;charset=utf-8;"});
  const href = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = href;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(href);
}
function openTasksExportDialog(){
  const u = currentSessionUser();
  if(!u || u.role!=="boss"){
    showSheet("Немає прав", `<div class="hint">Експорт доступний тільки керівнику.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  const today = kyivDateStr();
  showSheet("Експорт задач у Excel", `
    <div class="hint">Буде сформовано книгу Excel: окремі вкладки по відділах і <b>Особисті</b>, та вкладки <b>Аналітика (візуально)</b> + <b>Аналітика (таблично)</b>. Колонка <b>Оновлення</b> містить усю історію (обрізано).</div>
    <div class="row2">
      <div class="field">
        <label>Від дати</label>
        <input id="expFrom" type="date" value="${addDays(today, -30)}" />
      </div>
      <div class="field">
        <label>До дати</label>
        <input id="expTo" type="date" value="${today}" />
      </div>
    </div>
    <div class="actions" style="margin-top:14px;">
      <button class="btn primary" data-action="exportTasksExcelNow">⬇️ Завантажити Excel</button>
      <button class="btn ghost" data-action="hideSheet">Скасувати</button>
    </div>
  `);
}
function exportTasksExcelNow(){
  const from = document.getElementById("expFrom")?.value;
  const to = document.getElementById("expTo")?.value;
  if(!from || !to || from > to){
    showSheet("Помилка", `<div class="hint">Перевір період: дата “Від” має бути не пізніше “До”.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const visible = getVisibleTasksForUser(currentSessionUser()).filter(t=>taskInPeriod(t, from, to));
  const header = ["№","Код","Назва","Тип","Статус","Старт","Дедлайн","Контроль","Пріоритет","Відповідальний","Оновлено","Оновлення","Створив"];
  const groups = [
    ...STATE.departments.map(d=>({name: d.name, id: d.id})),
    {name: "Особисті", id: "personal"}
  ];
  const canUseXlsx = typeof XLSX !== "undefined" && XLSX.utils && XLSX.writeFile;
  if(canUseXlsx){
    const wb = XLSX.utils.book_new();
    const addSheet = (name, headerRow, rows)=>{
      const data = [headerRow, ...rows];
      const ws = XLSX.utils.aoa_to_sheet(data);
      ws["!cols"] = autoCols(data);
      applyTimesFont(ws);
      XLSX.utils.book_append_sheet(wb, ws, normalizeSheetName(name));
    };
    const addSheetRaw = (name, data)=>{
      const ws = XLSX.utils.aoa_to_sheet(data);
      ws["!cols"] = autoCols(data);
      applyTimesFont(ws);
      XLSX.utils.book_append_sheet(wb, ws, normalizeSheetName(name));
    };

    groups.forEach(g=>{
      const deptTasks = (g.id==="personal")
        ? visible.filter(t=>!t.departmentId)
        : visible.filter(t=>t.departmentId===g.id);
      if(!deptTasks.length) return;
      addSheet(g.name, header, taskExportRowsFull(deptTasks));
    });
    addSheetRaw("Аналітика (візуально)", buildAnalyticsRows());
    addSheetRaw("Аналітика (таблично)", buildAnalyticsTableRows());

    XLSX.writeFile(wb, `tasks_${from}_${to}.xlsx`, {cellStyles:true});
    hideSheet();
    return;
  }

  const sheets = [];
  groups.forEach(g=>{
    const deptTasks = (g.id==="personal")
      ? visible.filter(t=>!t.departmentId)
      : visible.filter(t=>t.departmentId===g.id);
    if(!deptTasks.length) return;
    sheets.push(buildWorksheetXml(g.name, header, taskExportRowsFull(deptTasks)));
  });
  sheets.push(buildWorksheetXmlRaw("Аналітика (візуально)", buildAnalyticsRows()));
  sheets.push(buildWorksheetXmlRaw("Аналітика (таблично)", buildAnalyticsTableRows()));
  const xml = buildTasksWorkbookXml(sheets);
  downloadExcelXml(`tasks_${from}_${to}.xml`, xml);
  hideSheet();
}

/* ===========================
   REPORTS + SUMMARIES
=========================== */
function getVisibleReportsForUser(u){
  if(!u) return [];
  if(u.role==="boss") return STATE.dailyReports.slice();
  return STATE.dailyReports.filter(r=>r.departmentId===u.departmentId);
}
function submitDailyReport({userId, doneText, progressText, blockedText}){
  const u = getUserById(userId);
  const date = kyivDateStr();
  const now = kyivNow();
  const late = (!isWeekend(now)) && minutesSinceMidnight(now) > REPORT_DEADLINE_MIN;

  const existing = STATE.dailyReports.find(r=>r.userId===userId && r.reportDate===date);
  const payload = {
    id: existing?.id || uid("rep"),
    reportDate: date,
    userId,
    departmentId: u.departmentId,
    doneText, progressText, blockedText,
    submittedAt: nowIsoKyiv(),
    isLate: late
  };
  if(existing){
    const idx = STATE.dailyReports.findIndex(r=>r.id===existing.id);
    STATE.dailyReports[idx] = payload;
  } else {
    STATE.dailyReports.push(payload);
  }
  saveState(STATE);
}
function submitDeptSummary({departmentId, authorUserId, text}){
  const date = kyivDateStr();
  const existing = STATE.deptSummaries.find(s=>s.departmentId===departmentId && s.summaryDate===date);
  const payload = {
    id: existing?.id || uid("sum"),
    summaryDate: date,
    departmentId,
    authorUserId,
    text: (text || "").trim(),
    submittedAt: nowIsoKyiv()
  };
  if(existing){
    const idx = STATE.deptSummaries.findIndex(s=>s.id===existing.id);
    STATE.deptSummaries[idx] = payload;
  } else {
    STATE.deptSummaries.push(payload);
  }
  saveState(STATE);
}

/* ===========================
   MODAL
=========================== */
const root = document.getElementById("root");
const modal = document.getElementById("modal");
const sheetTitle = document.getElementById("sheetTitle");
const sheetBody = document.getElementById("sheetBody");
document.getElementById("sheetClose").addEventListener("click", ()=>hideSheet());
modal.addEventListener("click", (e)=>{ if(e.target === modal) hideSheet(); });

function showSheet(title, html){
  sheetTitle.textContent = title;
  sheetBody.innerHTML = html;
  modal.classList.add("show");
}
function hideSheet(){
  modal.classList.remove("show");
  sheetBody.innerHTML = "";
}
let toastTimer = null;
function ensureToastContainer(){
  let el = document.getElementById("toastContainer");
  if(el) return el;
  el = document.createElement("div");
  el.id = "toastContainer";
  el.className = "toast-container";
  document.body.appendChild(el);
  return el;
}
function showToast(message, type="info"){
  const box = ensureToastContainer();
  box.innerHTML = `<div class="toast toast-${type}">${htmlesc(message)}</div>`;
  box.classList.add("show");
  if(toastTimer) clearTimeout(toastTimer);
  toastTimer = setTimeout(()=>{
    box.classList.remove("show");
    box.innerHTML = "";
  }, 1700);
}

/* ===========================
   ROUTING / UI
=========================== */
const ROUTES = {
  LOGIN: "login",
  CONTROL: "control",
  REPORTS: "reports",
  TASKS: "tasks",
  ANALYTICS: "analytics",
  PROFILE: "profile",
};
function loadTheme(){
  return safeGet(THEME_KEY) === "dark" ? "dark" : "light";
}
function applyTheme(theme){
  const dark = theme === "dark";
  document.body.classList.toggle("theme-dark", dark);
  document.body.classList.toggle("theme-light", !dark);
}
let UI = {
  route: ROUTES.LOGIN,
  tab: ROUTES.CONTROL,
  taskFilter: "активні",
  taskDeptFilter: "all",
  taskSearch: "",
  taskPersonalFilter: "all",
  taskIndexMap: {},
  reportFilter: "сьогодні",
  reportsControlDate: null, // NEW
  theme: loadTheme(),
};
function toggleTheme(){
  UI.theme = UI.theme === "dark" ? "light" : "dark";
  safeSet(THEME_KEY, UI.theme);
  applyTheme(UI.theme);
  render();
}

function ensureLoggedIn(){
  const u = currentSessionUser();
  if(!u){
    UI.route = ROUTES.LOGIN;
    return false;
  }
  return true;
}
function logout(){
  STATE.session.userId = null;
  saveState(STATE);
  UI.route = ROUTES.LOGIN;
  render();
}
function setTab(tab){
  UI.tab = tab;
  render();
}
function goProfile(){
  UI.route = ROUTES.PROFILE;
  render();
}

function appShell({title, subtitle, bodyHtml, showFab, fabAction, tabs}){
  const u = currentSessionUser();
  const banner = actingBannerForUser(u);
  const date = kyivDateStr();
  const weekend = isWeekend(kyivNow());
  const deadlineInfo = weekend ? "Вихідний" : "Дедлайн звіту: 17:30";
  const themeIcon = UI.theme === "dark" ? "☀️" : "🌙";
  const themeTitle = UI.theme === "dark" ? "Світла тема" : "Темна тема";
  const syncTitle = _syncReady ? "Дані завантажено" : (_syncInitDone ? "Дані не завантажено" : "Завантаження даних...");
  const syncDot = SYNC_URL ? `<span class="sync-dot ${_syncReady ? "ok" : "err"}" title="${syncTitle}"></span>` : ``;
  const compactTasks = !!(u && u.role==="boss" && UI.tab===ROUTES.TASKS && UI.taskDeptFilter && !["all","personal"].includes(UI.taskDeptFilter));
  const scopeAll = !!(u && u.role==="boss" && UI.tab===ROUTES.TASKS && UI.taskDeptFilter==="all");
  const scopeDept = !!(u && UI.tab===ROUTES.TASKS && (u.role!=="boss" || (u.role==="boss" && UI.taskDeptFilter && !["all","personal"].includes(UI.taskDeptFilter))));

  root.innerHTML = `
    <div class="app">
      <div class="topbar">
        <div class="topbar-inner">
          <div class="brand">
            <div class="logo">П</div>
            <div class="titleblock">
              <div class="h">${htmlesc(title)}</div>
              <div class="s">${htmlesc(subtitle)} • <span class="mono">${date}</span> • ${deadlineInfo}</div>
            </div>
          </div>
          <div class="top-actions">
            ${syncDot}
            <button class="iconbtn" data-action="openHelp" title="Довідка">❓</button>
            <button class="iconbtn" data-action="toggleTheme" title="${themeTitle}">${themeIcon}</button>
            <button class="iconbtn" data-action="goProfile" title="Профіль">👤</button>
          </div>
        </div>
      </div>

      ${banner ? `<div class="banner"><div>${htmlesc(banner)}</div><div class="mono">${date}</div></div>` : ``}

      <div class="content">${bodyHtml}</div>

      ${showFab ? `<button class="fab" id="fab">＋</button>` : ``}

      <div class="nav">
        <div class="nav-inner">
          ${renderTabs(tabs)}
        </div>
      </div>
    </div>
  `;

  document.body.classList.toggle("role-boss", !!(u && u.role==="boss"));
  document.body.classList.toggle("compact-tasks", compactTasks);
  document.body.classList.toggle("scope-all", scopeAll);
  document.body.classList.toggle("scope-dept", scopeDept);
  if(showFab){
    document.getElementById("fab").addEventListener("click", fabAction);
  }
}
function renderTabs(tabs){
  const u = currentSessionUser();
  const cls = (u && u.role==="boss") ? "tabs" : "tabs three";
  return `
    <div class="${cls}">
      ${tabs.map(t=>{
        const active = (UI.tab===t.key) ? "active" : "";
        return `
          <div class="tab ${active}" data-action="setTab" data-arg1="${t.key}" data-label="${htmlesc(t.label)}" title="${htmlesc(t.label)}" aria-label="${htmlesc(t.label)}">
            <div class="ico">${t.ico}</div>
            <div class="label">${htmlesc(t.label)}</div>
          </div>
        `;
      }).join("")}
    </div>
  `;
}

/* ===========================
   LOGIN VIEW
=========================== */
function viewLogin(){
  const themeIcon = UI.theme === "dark" ? "☀️" : "🌙";
  const themeTitle = UI.theme === "dark" ? "Світла тема" : "Темна тема";
  const syncLoading = !!SYNC_URL && !_syncInitDone;
  const syncTitle = _syncReady ? "Дані завантажено" : (_syncInitDone ? "Дані не завантажено" : "Завантаження даних...");
  const syncDot = SYNC_URL ? `<span class="sync-dot ${_syncReady ? "ok" : "err"}" title="${syncTitle}"></span>` : ``;
  document.body.classList.remove("role-boss");
  const html = `
    <div class="app">
      <div class="topbar">
        <div class="topbar-inner">
          <div class="brand">
            <div class="logo">П</div>
            <div class="titleblock">
              <div class="h">Планувальник</div>
              <div class="s">Prototype • localStorage • Українська</div>
            </div>
          </div>
          <div class="top-actions">
            <div class="pill mono">${kyivDateStr()}</div>
            ${syncDot}
            <button class="iconbtn" data-action="openHelp" title="Довідка">❓</button>
            <button class="iconbtn" data-action="toggleTheme" title="${themeTitle}">${themeIcon}</button>
          </div>
        </div>
      </div>

      <div class="content">
        <div class="card">
          <div class="card-h">
            <div class="t">Вхід</div>
            <span class="badge b-blue">Mobile-first</span>
          </div>
          <div class="card-b">
            <div class="field">
              <label>Логін</label>
              <input id="login" placeholder="boss / head2 / head5 / e21..." autocomplete="username" ${syncLoading ? "disabled" : ""} />
            </div>
            <div class="field">
              <label>Пароль</label>
              <input id="pass" placeholder="1234" type="password" autocomplete="current-password" ${syncLoading ? "disabled" : ""} />
            </div>

            <div class="actions" style="margin-top:14px;">
              <button class="btn primary" id="btnLogin" ${syncLoading ? "disabled" : ""}>УВІЙТИ</button>
              <button class="btn ghost" id="btnReset">Скинути дані</button>
            </div>

            ${syncLoading ? `<div class="hint">Завантаження даних з хмари...</div>` : ``}
            <div class="hint">
              Демо-акаунти:<br/>
              <span class="mono">boss / 1234</span> (керівник)<br/>
              <span class="mono">head2 / 1234</span> (нач. відділу №2)<br/>
              <span class="mono">head5 / 1234</span> (нач. відділу №5)<br/>
              Виконавці: <span class="mono">e21/e22/e51/e41 / 1234</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  `;
  root.innerHTML = html;

  document.getElementById("btnLogin").addEventListener("click", ()=>{
    const login = document.getElementById("login").value.trim();
    const pass = document.getElementById("pass").value.trim();
    const user = STATE.users.find(u=>u.login===login && u.pass===pass && u.active);
    if(!user){
      showSheet("Помилка входу", `<div class="hint">Невірний логін або пароль.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
      return;
    }
    recomputeDelegationStatuses();
    STATE.session.userId = user.id;
    saveState(STATE);
    UI.tab = ROUTES.CONTROL;
    render();
  });

  document.getElementById("btnReset").addEventListener("click", ()=>{
    safeRemove(LS_KEY);
    STATE = seed();
    showSheet("Готово", `<div class="hint">Дані скинуто. Завантажено демо.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
  });
}

/* ===========================
   CONTROL VIEW
=========================== */
function viewControl(){
  if(!ensureLoggedIn()) return viewLogin();
  recomputeDelegationStatuses();

  const u = currentSessionUser();
  UI.tab = ROUTES.CONTROL;

  const today = kyivDateStr();
  const weekend = isWeekend(kyivNow());

  const visibleExecutors = (u.role==="boss")
    ? STATE.users.filter(x=>x.active && x.role==="executor")
    : STATE.users.filter(x=>x.active && x.role==="executor" && x.departmentId===u.departmentId);

  const reportsToday = STATE.dailyReports.filter(r=>r.reportDate===today);
  const missing = weekend ? [] : visibleExecutors.filter(x=>!reportsToday.some(r=>r.userId===x.id));
  const late = weekend ? [] : reportsToday.filter(r=>{
    const usr = getUserById(r.userId);
    if(!usr) return false;
    if(u.role!=="boss" && usr.departmentId!==u.departmentId) return false;
    return r.isLate;
  });

  const tasksVis = getVisibleTasksForUser(u);
  const requestClose = tasksVis.filter(t=>t.type==="managerial" && t.status==="очікує_підтвердження");
  const overdue = tasksVis.filter(t=>isOverdue(t));
  const notBlocked = (t)=>!["блокер","очікування"].includes(t.status);
  const isDeptTask = (t)=>!!t.departmentId;
  const controlTasksDate = tasksVis.filter(t=>t.nextControlDate && !t.controlAlways && notBlocked(t) && isDeptTask(t));
  const controlTasksAlways = tasksVis.filter(t=>t.controlAlways && !t.nextControlDate && notBlocked(t) && isDeptTask(t));
  const deadlineTasks = tasksVis.filter(t=>t.dueDate && isDeptTask(t) && t.status!=="закрито" && t.status!=="скасовано");
  const controlByDeptDate = (u.role==="boss")
    ? STATE.departments.map(d=>{
        const list = controlTasksDate.filter(t=>t.departmentId===d.id);
        return {dept:d, count:list.length};
      }).filter(x=>x.count>0)
    : [];
  const controlByDeptAlways = (u.role==="boss")
    ? STATE.departments.map(d=>{
        const list = controlTasksAlways.filter(t=>t.departmentId===d.id);
        return {dept:d, count:list.length};
      }).filter(x=>x.count>0)
    : [];
  const controlByDeptDeadline = (u.role==="boss")
    ? STATE.departments.map(d=>{
        const list = deadlineTasks.filter(t=>t.departmentId===d.id);
        return {dept:d, count:list.length};
      }).filter(x=>x.count>0)
    : [];
  const blockers = tasksVis.filter(t=>["блокер","очікування"].includes(t.status));
  const stale = tasksVis.filter(t=>staleTask(t, 7));
  const activeDelegations = (u.role==="boss") ? STATE.delegations.filter(d=>d.status==="активне") : [];

  const {isDeptHeadLike} = asDeptRole(u);

  let summaryBadge = "";
  if(u.role!=="boss"){
    const sum = STATE.deptSummaries.find(s=>s.departmentId===u.departmentId && s.summaryDate===today);
    summaryBadge = sum ? `<span class="badge b-ok">✅ Підсумок подано</span>` : `<span class="badge b-warn">🟡 Підсумку немає</span>`;
  }

  const body = `
    <div class="card">
      <div class="card-h">
        <div class="t">Контроль</div>
        <span class="badge ${weekend ? "b-warn":"b-blue"}">${weekend ? "Вихідний" : "Будній"}</span>
      </div>
      <div class="card-b">
        <div class="statlist">
          ${u.role==="boss" ? `
          <div class="stat" data-action="openControlByDept">
            <div class="l">
              <div class="k">🎯 На контролі по відділах</div>
              <div class="d control-lines">
                ${
                  (!controlByDeptDate.length && !controlByDeptAlways.length && !controlByDeptDeadline.length)
                    ? `<span>Немає задач на контролі</span>`
                    : ``
                }
              </div>
            </div>
            <div class="r control-counts">
              <span class="pill mono">⏱ ${deadlineTasks.length}</span>
              <span class="pill mono">🗓 ${controlTasksDate.length}</span>
              <span class="pill mono">🎯 ${controlTasksAlways.length}</span>
            </div>
          </div>
          ` : ``}

          <div class="stat" data-action="openMissing">
            <div class="l">
              <div class="k">🔴 Не здали до 17:30</div>
              <div class="d">${u.role==="boss" ? "По всіх відділах" : "По вашому відділу"}</div>
            </div>
            <div class="r"><span class="mono">${missing.length}</span> ›</div>
          </div>

          <div class="stat" data-action="openTaskList" data-arg1="очікує_підтвердження">
            <div class="l">
              <div class="k">🟣 Очікує підтвердження</div>
              <div class="d">Управлінські задачі</div>
            </div>
            <div class="r"><span class="mono">${requestClose.length}</span> ›</div>
          </div>

          <div class="stat" data-action="openTaskList" data-arg1="прострочені">
            <div class="l">
              <div class="k">🟠 Прострочені задачі</div>
              <div class="d">Є дедлайн і він минув</div>
            </div>
            <div class="r"><span class="mono">${overdue.length}</span> ›</div>
          </div>

          <div class="stat" data-action="openTaskList" data-arg1="блокери">
            <div class="l">
              <div class="k">🟡 Блокери / очікування</div>
              <div class="d">Потребує уваги</div>
            </div>
            <div class="r"><span class="mono">${blockers.length}</span> ›</div>
          </div>

          <div class="stat" data-action="openTaskList" data-arg1="без_оновлень">
            <div class="l">
              <div class="k">⏳ Без оновлення &gt; 7 днів</div>
              <div class="d">Довгі задачі “висять”</div>
            </div>
            <div class="r"><span class="mono">${stale.length}</span> ›</div>
          </div>

          ${u.role==="boss" ? `
          <div class="stat" data-action="openDelegations">
            <div class="l">
              <div class="k">🧩 Активні заміщення (в.о.)</div>
              <div class="d">Хто зараз виконує обов’язки</div>
            </div>
            <div class="r"><span class="mono">${activeDelegations.length}</span> ›</div>
          </div>
          ` : `
          <div class="stat" data-action="openDeptPeople">
            <div class="l">
              <div class="k">👥 Люди / звітність сьогодні</div>
              <div class="d">Хто здав / не здав</div>
            </div>
            <div class="r">›</div>
          </div>
          `}
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-h">
        <div class="t">Швидкі дії</div>
        <span class="badge b-blue">1–2 натискання</span>
      </div>
      <div class="card-b">
        <div class="actions">
          <button class="btn ghost" data-action="openTaskList" data-arg1="активні">📌 Активні задачі</button>

          ${u.role==="boss" ? `
            <button class="btn ghost" data-action="openCreateTask" data-arg1="personal">➕ Моя задача</button>
            <button class="btn ghost" data-action="openCreateTask" data-arg1="managerial">
              <span class="qa-full">➕ Управлінська задача</span>
              <span class="qa-short">➕ Управлінськ</span>
            </button>
          ` : `
            <button class="btn ghost" data-action="openCreateTask" data-arg1="internal">➕ Внутрішня задача</button>
          `}
        </div>

        ${u.role!=="boss" ? `` : `
          <div class="hint" style="margin-top:10px;">Порада: “Мої задачі” — дзвінки/нагадування/контроль без дедлайну. Став контрольну дату.</div>
        `}
      </div>
    </div>
  `;

  const tabs = (u.role==="boss")
    ? [
      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},
      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},
      {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},
    ]
    : [
      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},
      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},
    ];

  const subtitle = (u.role==="boss") ? "Керівник" : (getDeptById(u.departmentId)?.name ?? "Відділ");
  const fabAction = ()=>{
    if(u.role==="boss"){
      showSheet("Додати", `
        <div class="actions">
          <button class="btn primary" data-action="hideThen" data-next="openCreateTask" data-arg1="personal">➕ Моя задача</button>
          <button class="btn ghost" data-action="hideThen" data-next="openCreateTask" data-arg1="managerial">➕ Управлінська</button>
        </div>
        <div class="sep"></div>
        <button class="btn ghost" data-action="hideSheet">Закрити</button>
      `);
    } else {
      openCreateTask('internal');
    }
  };

  appShell({
    title: "Контроль",
    subtitle,
    bodyHtml: body,
    showFab: true,
    fabAction,
    tabs
  });
}

/* ===========================
   DEPT PEOPLE (Начальник/в.о.)
=========================== */
function openDeptPeople(){
  const u = currentSessionUser();
  const {isDeptHeadLike} = asDeptRole(u);
  if(u.role==="boss"){
    showSheet("Люди/штат", `<div class="hint">Цей екран потрібен саме для начальника відділу (або в.о.). У керівника є “👥 Люди” у вкладці “Звіти”.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  if(!isDeptHeadLike){
    showSheet("Немає прав", `<div class="hint">Тільки начальник відділу (або в.о.) може переглядати “Люди/штат”.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const deptId = u.departmentId;
  const dept = getDeptById(deptId);
  const today = kyivDateStr();
  const weekend = isWeekend(kyivNow());

  const people = STATE.users
    .filter(x=>x.active && x.departmentId===deptId)
    .sort((a,b)=>a.role.localeCompare(b.role));

  const repsToday = STATE.dailyReports.filter(r=>r.reportDate===today && r.departmentId===deptId);

  const rows = people.map(p=>{
    const expected = (p.role==="executor") ? (!weekend) : false;
    const rep = repsToday.find(r=>r.userId===p.id) || null;

    let badge = `<span class="badge">—</span>`;
    if(expected){
      if(!rep) badge = `<span class="badge b-danger">🔴 НЕ ЗДАВ</span>`;
      else badge = rep.isLate ? `<span class="badge b-warn">🟡 ПІЗНО</span>` : `<span class="badge b-ok">✅ ВЧАСНО</span>`;
    } else {
      if(rep) badge = rep.isLate ? `<span class="badge b-warn">🟡 (є звіт, пізно)</span>` : `<span class="badge b-ok">✅ (є звіт)</span>`;
      else badge = `<span class="badge">не очікується</span>`;
    }

    const roleLabel = (p.role==="dept_head") ? "начальник" : (p.role==="executor" ? "виконавець" : p.role);
    const acting = isActingHead(p.id) ? " (в.о.)" : "";

    return `
      <div class="item" style="cursor:default;">
        <div class="row">
          <div>
            <div class="name">${htmlesc(p.name)}${acting}</div>
            <div class="sub">
              <span class="pill">${roleLabel}</span>
              ${badge}
              ${rep ? `<span class="pill mono">${htmlesc(rep.submittedAt.slice(11,16))}</span>` : ``}
            </div>
            ${rep ? `<div class="hint" style="margin-top:8px;"><b>Коротко:</b> ${htmlesc(shorten(rep.doneText, 90))}</div>` : ``}
          </div>
        </div>
      </div>
    `;
  }).join("");

  showSheet(`Люди/штат — ${dept?.name ?? ""}`, `
    <div class="hint">
      Показано статус звітності за <span class="mono">${fmtDate(today)}</span>.
      ${weekend ? "Сьогодні вихідний — звіти не обов’язкові." : "Очікуємо звіт лише від виконавців."}
    </div>
    <div class="sep"></div>
    ${rows || `<div class="hint">Немає людей у відділі.</div>`}
    <div class="sep"></div>
    <div class="actions">
      <button class="btn ghost" data-action="hideSheet">Закрити</button>
    </div>
  `);
}

/* ===========================
   DEPT PEOPLE (КЕРІВНИК) — NEW: по даті
=========================== */
function openDeptPeopleBoss(deptId, dateStr){
  const u = currentSessionUser();
  if(!u || u.role!=="boss"){
    showSheet("Немає прав", `<div class="hint">Цей екран доступний тільки керівнику.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const dept = getDeptById(deptId);
  const day = dateStr || kyivDateStr();
  const weekend = isWeekend(new Date(day + "T12:00:00"));

  const people = STATE.users
    .filter(x=>x.active && x.departmentId===deptId)
    .sort((a,b)=>a.role.localeCompare(b.role));

  const reps = STATE.dailyReports.filter(r=>r.reportDate===day && r.departmentId===deptId);

  const rows = people.map(p=>{
    const expected = (p.role==="executor") ? (!weekend) : false;
    const rep = reps.find(r=>r.userId===p.id) || null;

    let badge = `<span class="badge">—</span>`;
    if(expected){
      if(!rep) badge = `<span class="badge b-danger">🔴 НЕ ЗДАВ</span>`;
      else badge = rep.isLate ? `<span class="badge b-warn">🟡 ПІЗНО</span>` : `<span class="badge b-ok">✅ ВЧАСНО</span>`;
    } else {
      if(rep) badge = rep.isLate ? `<span class="badge b-warn">🟡 (є звіт, пізно)</span>` : `<span class="badge b-ok">✅ (є звіт)</span>`;
      else badge = `<span class="badge">не очікується</span>`;
    }

    const roleLabel = (p.role==="dept_head") ? "начальник" : (p.role==="executor" ? "виконавець" : p.role);
    const acting = isActingHead(p.id) ? " (в.о.)" : "";

    return `
      <div class="item" style="cursor:default;">
        <div class="row">
          <div>
            <div class="name">${htmlesc(p.name)}${acting}</div>
            <div class="sub">
              <span class="pill">${roleLabel}</span>
              ${badge}
              ${rep ? `<span class="pill mono">${htmlesc(rep.submittedAt.slice(11,16))}</span>` : ``}
            </div>
            ${rep ? `<div class="hint" style="margin-top:8px;"><b>Коротко:</b> ${htmlesc(shorten(rep.doneText, 90))}</div>` : ``}
          </div>
        </div>
      </div>
    `;
  }).join("");

  showSheet(`Люди — ${deptShortLabel(dept)}`, `
    <div class="hint">
      Дата: <span class="mono">${fmtDate(day)}</span>.
      ${weekend ? "Вихідний — звіти не обов’язкові." : "Очікуємо звіт від виконавців."}
    </div>
    <div class="sep"></div>
    ${rows || `<div class="hint">Немає людей у відділі.</div>`}
    <div class="sep"></div>
    <button class="btn primary" data-action="hideSheet">Закрити</button>
  `);
}

/* ===========================
   DEPT SUMMARY FORM
=========================== */
function openDeptSummaryForm(){
  const u = currentSessionUser();
  const {isDeptHeadLike} = asDeptRole(u);
  if(u.role==="boss"){
    showSheet("Підсумок відділу", `<div class="hint">Підсумок подає начальник відділу (або в.о.).</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  if(!isDeptHeadLike){
    showSheet("Немає прав", `<div class="hint">Тільки начальник відділу (або в.о.) може подати підсумок.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const today = kyivDateStr();
  const existing = STATE.deptSummaries.find(s=>s.departmentId===u.departmentId && s.summaryDate===today);

  showSheet("Підсумок відділу", `
    <div class="hint">
      3–5 речень. Ключове: виконано/ризики/що потребує рішення.
    </div>
    <div class="field">
      <label>Текст підсумку</label>
      <textarea id="sumText" maxlength="600" placeholder="Приклад: За день виконано … / В процесі … / Ризик: … / Потрібно рішення: …">${htmlesc(existing?.text || "")}</textarea>
    </div>
    <div class="hint">Ліміт: 600 символів. Дата: <span class="mono">${fmtDate(today)}</span>.</div>
    <div class="actions" style="margin-top:14px;">
      <button class="btn primary" data-action="submitDeptSummaryNow">Надіслати</button>
      <button class="btn ghost" data-action="hideSheet">Скасувати</button>
    </div>
  `);
}

function submitDeptSummaryNow(){
  const u = currentSessionUser();
  const text = document.getElementById("sumText").value.trim();
  if(text.length < 10){
    showSheet("Помилка", `<div class="hint">Напиши хоча б кілька речень (мінімум 10 символів).</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  submitDeptSummary({departmentId:u.departmentId, authorUserId:u.id, text});
  hideSheet();
  render();
}

/* ===========================
   REPORT FORM
=========================== */
function openReportForm(){
  const u = currentSessionUser();
  const now = kyivNow();
  const date = kyivDateStr(now);
  const weekend = isWeekend(now);
  const mins = minutesSinceMidnight(now);
  const late = (!weekend) && mins > REPORT_DEADLINE_MIN;

  const existing = STATE.dailyReports.find(r=>r.userId===u.id && r.reportDate===date);

  showSheet("Щоденний звіт", `
    <div class="hint">
      Звіт за <span class="mono">${fmtDate(date)}</span>.
      ${weekend ? "Сьогодні вихідний — подання не обов’язкове." : (late ? "<b>Увага:</b> після 17:30 буде “ПІЗНО”." : "Подай до 17:30, щоб було вчасно.")}
    </div>
    <div class="actions" style="margin-top:10px;">
      <button class="btn ghost" data-action="applyReportTemplate">🧩 Шаблон</button>
      <button class="btn ghost" data-action="autoFillReport">⚡ Авто з задач</button>
    </div>

    <div class="field">
      <label>Що виконано</label>
      <textarea id="rDone" placeholder="Коротко, по пунктах. Можеш вставляти коди задач: T-2026-0004">${htmlesc(existing?.doneText || "")}</textarea>
    </div>
    <div class="field">
      <label>Що в процесі</label>
      <textarea id="rProg" placeholder="Що робиться зараз, що залишилось.">${htmlesc(existing?.progressText || "")}</textarea>
    </div>
    <div class="field">
      <label>Проблеми / блокери</label>
      <textarea id="rBlock" placeholder="Що заважає, кого/чого чекаємо.">${htmlesc(existing?.blockedText || "")}</textarea>
    </div>

    <div class="actions" style="margin-top:14px;">
      <button class="btn primary" data-action="submitReportNow">Надіслати</button>
      <button class="btn ghost" data-action="hideSheet">Скасувати</button>
    </div>
  `);
}

function submitReportNow(){
  const u = currentSessionUser();
  const doneText = document.getElementById("rDone").value.trim();
  const progressText = document.getElementById("rProg").value.trim();
  const blockedText = document.getElementById("rBlock").value.trim();
  submitDailyReport({userId:u.id, doneText, progressText, blockedText});
  hideSheet();
  render();
}

/* ===========================
   REPORTS VIEW (UPDATED with Control Block for Boss)
=========================== */
function setReportFilter(k){ UI.reportFilter = k; render(); }

function setReportsControlDate(v){
  if(!v) return;
  UI.reportsControlDate = v;
  render();
}
function setReportsControlDateFromInput(){
  const input = document.getElementById("ctrlDateInput");
  if(!input) return;
  setReportsControlDate(input.value);
}

function viewReports(){
  if(!ensureLoggedIn()) return viewLogin();
  recomputeDelegationStatuses();

  const u = currentSessionUser();
  UI.tab = ROUTES.REPORTS;

  const filter = UI.reportFilter;
  const today = kyivDateStr();
  let start = today;
  if(filter==="тиждень") start = addDays(today, -6);
  if(filter==="місяць") start = addDays(today, -29);

  if(UI.reportsControlDate == null) UI.reportsControlDate = today;
  const ctrlDate = UI.reportsControlDate;

  const reports = getVisibleReportsForUser(u)
    .filter(r=>r.reportDate >= start && r.reportDate <= today)
    .sort((a,b)=> (b.reportDate + b.submittedAt).localeCompare(a.reportDate + a.submittedAt));

  const sums = (u.role==="boss" ? STATE.deptSummaries : STATE.deptSummaries.filter(s=>s.departmentId===u.departmentId))
    .filter(s=>s.summaryDate >= start && s.summaryDate <= today)
    .sort((a,b)=> (b.summaryDate + b.submittedAt).localeCompare(a.summaryDate + a.submittedAt));

  const chips = `
    <div class="chips">
      <div class="chip ${filter==="сьогодні"?"active":""}" data-action="setReportFilter" data-arg1="сьогодні">Сьогодні</div>
      <div class="chip ${filter==="тиждень"?"active":""}" data-action="setReportFilter" data-arg1="тиждень">Тиждень</div>
      <div class="chip ${filter==="місяць"?"active":""}" data-action="setReportFilter" data-arg1="місяць">Місяць</div>
    </div>
  `;

  function computeDeptControl(dateStr){
    const weekend = isWeekend(new Date(dateStr + "T12:00:00"));
    const reps = STATE.dailyReports.filter(r=>r.reportDate===dateStr);

    return STATE.departments.map(d=>{
      const executors = STATE.users.filter(x=>x.active && x.role==="executor" && x.departmentId===d.id);
      const deptReps = reps.filter(r=>r.departmentId===d.id);

      const missing = weekend ? [] : executors.filter(ex=>!deptReps.some(r=>r.userId===ex.id));
      const late = weekend ? [] : deptReps.filter(r=>r.isLate && executors.some(ex=>ex.id===r.userId));

      return {
        deptId: d.id,
        deptName: d.name,
        weekend,
        expected: executors.length,
        missingCount: missing.length,
        lateCount: late.length
      };
    });
  }

  const deptControl = (u.role==="boss") ? computeDeptControl(ctrlDate) : [];

  const controlBlock = (u.role==="boss") ? `
    <div class="item" style="cursor:default;">
      <div class="row">
        <div class="name">🧭 Контроль подання звітів</div>
        <span class="badge b-blue">Керівник</span>
      </div>
      <div class="hint" style="margin-top:10px;">
        Обери дату — і одразу видно “хто не здав” по відділах.
      </div>

      <div class="field" style="margin-top:12px;">
        <label>Дата</label>
        <input type="date" id="ctrlDateInput" value="${ctrlDate}" data-change="setReportsControlDateFromInput" />
      </div>
    </div>

    <div class="list">
      ${deptControl.map(x=>{
        const missBadge = x.weekend ? `<span class="badge">Вихідний</span>` :
          (x.missingCount ? `<span class="badge b-danger">🔴 ${x.missingCount} не здали</span>` : `<span class="badge b-ok">✅ всі здали</span>`);
        const lateBadge = (!x.weekend && x.lateCount) ? `<span class="badge b-warn">🟡 ${x.lateCount} пізно</span>` : ``;

        return `
          <div class="item" style="cursor:default;">
            <div class="row">
              <div>
            <div class="name">${deptBadgeHtml(getDeptById(x.deptId))}</div>
                <div class="sub">
                  ${missBadge} ${lateBadge}
                  <span class="pill">Виконавців: <span class="mono">${x.expected}</span></span>
                </div>
              </div>
              <button class="btn ghost" data-action="openDeptPeopleBoss" data-arg1="${x.deptId}" data-arg2="${ctrlDate}">👥 Люди</button>
            </div>
          </div>
        `;
      }).join("")}
    </div>

    <div class="sep"></div>
  ` : ``;

  const listReports = reports.length ? reports.map(r=>{
    const usr = getUserById(r.userId);
    const dept = getDeptById(r.departmentId);
    const badge = r.isLate ? `<span class="badge b-warn">🟡 ПІЗНО</span>` : `<span class="badge b-ok">✅ ВЧАСНО</span>`;
    return `
      <div class="item" data-action="openReport" data-arg1="${r.id}">
        <div class="row">
          <div>
            <div class="name">${deptBadgeHtml(dept)} — ${htmlesc(usr?.name ?? "")}</div>
            <div class="sub">
              ${badge}
              <span class="pill mono">${fmtDate(r.reportDate)}</span>
              <span class="pill mono">${htmlesc(r.submittedAt.slice(11,16))}</span>
            </div>
          </div>
          <div class="pill">›</div>
        </div>
        <div class="hint" style="margin-top:10px;">
          <b>Виконано:</b> ${htmlesc(shorten(r.doneText))}<br/>
          <b>Блокери:</b> ${htmlesc(shorten(r.blockedText))}
        </div>
      </div>
    `;
  }).join("") : `<div class="hint">Немає звітів за обраний період. Спробуй інший фільтр або перевір, чи подані звіти.</div>`;

  const listSums = sums.length ? sums.map(s=>{
    const dept = getDeptById(s.departmentId);
    const au = getUserById(s.authorUserId);
    return `
      <div class="item" data-action="openDeptSummary" data-arg1="${s.id}">
        <div class="row">
          <div>
            <div class="name">🧾 Підсумок — ${deptBadgeHtml(dept)}</div>
            <div class="sub">
              <span class="badge b-violet">Підсумок відділу</span>
              <span class="pill mono">${fmtDate(s.summaryDate)}</span>
              <span class="pill">${htmlesc(au?.name ?? "")}${isActingHead(au?.id) ? " (в.о.)" : ""}</span>
              <span class="pill mono">${htmlesc(s.submittedAt.slice(11,16))}</span>
            </div>
          </div>
          <div class="pill">›</div>
        </div>
        <div class="hint" style="margin-top:10px;">${htmlesc(shorten(s.text, 140))}</div>
      </div>
    `;
  }).join("") : `<div class="hint">Немає підсумків за обраний період. Спробуй інший фільтр або період.</div>`;

  const body = `
    <div class="card">
      <div class="card-h">
        <div class="t">Звіти</div>
        <span class="badge b-blue">${u.role==="boss" ? "Всі відділи" : "Мій відділ"}</span>
      </div>
      <div class="card-b">
        ${chips}
        <div class="sep"></div>

        ${controlBlock}

        <div class="item" style="cursor:default;">
          <div class="row"><div class="name">🧾 Підсумки відділів</div><span class="badge b-violet mono">${sums.length}</span></div>
          <div class="hint">Короткий підсумок (3–5 речень) від начальника/в.о.</div>
        </div>
        <div class="list">${listSums}</div>

        <div class="sep"></div>
        <div class="item" style="cursor:default;">
          <div class="row"><div class="name">📝 Щоденні звіти</div><span class="badge b-blue mono">${reports.length}</span></div>
          <div class="hint">Звіти виконавців (вчасно/пізно).</div>
        </div>
        <div class="list">${listReports}</div>
      </div>
    </div>
  `;

  const tabs = (u.role==="boss")
    ? [
      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},
      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},
      {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},
    ]
    : [
      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},
      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},
    ];

  const subtitle = (u.role==="boss") ? "Керівник" : (getDeptById(u.departmentId)?.name ?? "Відділ");
  appShell({title:"Звіти", subtitle, bodyHtml: body, showFab:false, fabAction:null, tabs});
}

function openReport(reportId){
  const u = currentSessionUser();
  const r = STATE.dailyReports.find(x=>x.id===reportId);
  if(!r) return;

  if(u.role!=="boss" && !canAccessDept(u, r.departmentId)){
    showSheet("Немає доступу", `<div class="hint">Цей звіт належить іншому відділу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  const usr = getUserById(r.userId);
  const dept = getDeptById(r.departmentId);

  showSheet("Звіт", `
    <div class="item" style="cursor:default;">
      <div class="row">
        <div>
          <div class="name">${htmlesc(dept?.name ?? "")} — ${htmlesc(usr?.name ?? "")}</div>
          <div class="sub">
            <span class="pill mono">${fmtDate(r.reportDate)}</span>
            <span class="pill mono">${htmlesc(r.submittedAt)}</span>
            ${r.isLate ? `<span class="badge b-warn">🟡 ПІЗНО</span>` : `<span class="badge b-ok">✅ ВЧАСНО</span>`}
          </div>
        </div>
      </div>
    </div>

    <div class="sep"></div>

    <div class="item" style="cursor:default;">
      <div class="name">✔ Виконано</div>
      <div class="hint" style="margin-top:10px; white-space:pre-wrap;">${htmlesc(r.doneText || "—")}</div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">⏳ В процесі</div>
      <div class="hint" style="margin-top:10px; white-space:pre-wrap;">${htmlesc(r.progressText || "—")}</div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">⛔ Блокери</div>
      <div class="hint" style="margin-top:10px; white-space:pre-wrap;">${htmlesc(r.blockedText || "—")}</div>
    </div>

    <div class="sep"></div>
    <button class="btn primary" data-action="hideSheet">Закрити</button>
  `);
}

function openDeptSummary(summaryId){
  const u = currentSessionUser();
  const s = STATE.deptSummaries.find(x=>x.id===summaryId);
  if(!s) return;

  if(u.role!=="boss" && s.departmentId !== u.departmentId){
    showSheet("Немає доступу", `<div class="hint">Цей підсумок належить іншому відділу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const dept = getDeptById(s.departmentId);
  const au = getUserById(s.authorUserId);

  showSheet("Підсумок відділу", `
    <div class="item" style="cursor:default;">
      <div class="row">
        <div>
          <div class="name">${htmlesc(dept?.name ?? "")}</div>
          <div class="sub">
            <span class="badge b-violet">Підсумок</span>
            <span class="pill mono">${fmtDate(s.summaryDate)}</span>
            <span class="pill">${htmlesc(au?.name ?? "")}${isActingHead(au?.id) ? " (в.о.)" : ""}</span>
            <span class="pill mono">${htmlesc(s.submittedAt)}</span>
          </div>
        </div>
      </div>
    </div>

    <div class="sep"></div>

    <div class="item" style="cursor:default;">
      <div class="name">Текст</div>
      <div class="hint" style="margin-top:10px; white-space:pre-wrap;">${htmlesc(s.text || "—")}</div>
    </div>

    <div class="sep"></div>
    <button class="btn primary" data-action="hideSheet">Закрити</button>
  `);
}

/* ===========================
   TASKS VIEW + ACTIONS
=========================== */
function setTaskFilter(k){ UI.taskFilter = k; render(); }
function setTaskDeptFilter(k){ UI.taskDeptFilter = k; render(); }
function setTaskPersonalFilter(k){ UI.taskPersonalFilter = k; render(); }
function toggleTaskScope(){
  UI.taskDeptFilter = (UI.taskDeptFilter === "personal") ? "all" : "personal";
  render();
}
function setTaskSearchFromInput(){
  const input = document.getElementById("taskSearchInput");
  UI.taskSearch = (input?.value || "").trim().toLowerCase();
  render();
}
function clearTaskSearch(){
  UI.taskSearch = "";
  render();
}

function viewTasks(){
  if(!ensureLoggedIn()) return viewLogin();
  recomputeDelegationStatuses();
  const u = currentSessionUser();
  const {isDeptHeadLike} = asDeptRole(u);
  UI.tab = ROUTES.TASKS;

  let tasks = getVisibleTasksForUser(u);
  const filter = UI.taskFilter;
  const deptFilter = UI.taskDeptFilter || "all";
  const taskSearch = UI.taskSearch || "";
  const personalFilter = UI.taskPersonalFilter || "all";
  const showAnnouncementsScope = (u.role!=="boss") || (u.role==="boss" && deptFilter==="personal");
  const effectivePersonalFilter = showAnnouncementsScope ? personalFilter : "tasks";
  let announcements = showAnnouncementsScope ? getVisibleAnnouncementsForUser(u) : [];
  const deptLabel = (t)=> t.departmentId ? (getDeptById(t.departmentId)?.name || "Відділ") : "Особисто";
  const matchesSearch = (t)=>{
    if(!taskSearch) return true;
    const dept = t.departmentId ? getDeptById(t.departmentId)?.name : "Особисто";
    const resp = getUserById(t.responsibleUserId)?.name || "";
    const aud = isAnnouncement(t) ? announcementAudienceLabel(t.audience) : "";
    const hay = `${t.title} ${t.id} ${dept} ${resp} ${aud}`.toLowerCase();
    return hay.includes(taskSearch);
  };
  const highlightMatch = (text)=>{
    const raw = String(text ?? "");
    if(!taskSearch) return htmlesc(raw);
    const needle = taskSearch;
    const lower = raw.toLowerCase();
    if(!needle || !lower.includes(needle)) return htmlesc(raw);
    let out = "";
    let i = 0;
    while(true){
      const idx = lower.indexOf(needle, i);
      if(idx === -1){
        out += htmlesc(raw.slice(i));
        break;
      }
      out += htmlesc(raw.slice(i, idx));
      out += `<mark class="search-hit">${htmlesc(raw.slice(idx, idx + needle.length))}</mark>`;
      i = idx + needle.length;
    }
    return out;
  };

  if(u.role === "boss"){
    if(deptFilter === "personal"){
      tasks = tasks.filter(t=>t.type==="personal");
    } else if(deptFilter !== "all"){
      tasks = tasks.filter(t=>t.departmentId === deptFilter);
    }
  }

  const isDeptScope = (u.role!=="boss") || (u.role==="boss" && deptFilter!=="all" && deptFilter!=="personal");
  const taskSort = (a,b)=>{
    const bucket = (t)=>{
      if(t.dueDate) return 0;
      if(["блокер","очікування"].includes(t.status)) return 1;
      if(t.nextControlDate) return 2;
      if(t.controlAlways) return 3;
      return 4;
    };
    const dateKey = (t)=>{
      if(t.dueDate) return dueSortKey(t.dueDate);
      if(t.nextControlDate) return t.nextControlDate;
      if(t.controlAlways) return "0000-00-00";
      return "9999-99-99";
    };
    if(u.role==="boss" && deptFilter==="all"){
      const deptName = (t)=> t.departmentId ? (getDeptById(t.departmentId)?.name || "Відділ") : "Особисто";
      const deptKey = (t)=> `${t.departmentId ? "0" : "1"}_${deptName(t)}`;
      const dk = deptKey(a).localeCompare(deptKey(b));
      if(dk!==0) return dk;
      const ba = bucket(a);
      const bb = bucket(b);
      if(ba!==bb) return ba - bb;
      const dka = dateKey(a);
      const dkb = dateKey(b);
      if(dka!==dkb) return dka.localeCompare(dkb);
      return (a.title || "").localeCompare(b.title || "");
    }
    if(isDeptScope){
      const ba = bucket(a);
      const bb = bucket(b);
      if(ba!==bb) return ba - bb;
      const dka = dateKey(a);
      const dkb = dateKey(b);
      if(dka!==dkb) return dka.localeCompare(dkb);
      return (a.title || "").localeCompare(b.title || "");
    }

    const ar = (a.status==="очікує_підтвердження") ? 0 : 1;
    const br = (b.status==="очікує_підтвердження") ? 0 : 1;
    if(ar!==br) return ar-br;
    const ao = isOverdue(a) ? 0 : 1;
    const bo = isOverdue(b) ? 0 : 1;
    if(ao!==bo) return ao-bo;
    const anc = controlSortKey(a);
    const bnc = controlSortKey(b);
    if(anc!==bnc) return anc.localeCompare(bnc);
    const ad = dueSortKey(a.dueDate);
    const bd = dueSortKey(b.dueDate);
    return ad.localeCompare(bd);
  };

  const filterFn = (t)=>{
    if(filter==="активні") return t.status!=="закрито" && t.status!=="скасовано";
    if(filter==="прострочені") return isOverdue(t);
    if(filter==="очікує_підтвердження") return t.type==="managerial" && t.status==="очікує_підтвердження";
    if(filter==="блокери") return ["блокер","очікування"].includes(t.status);
    if(filter==="без_оновлень") return staleTask(t,7);
    if(filter==="закриті") return t.status==="закрито";
    return true;
  };

  const filtered = tasks.filter(filterFn).filter(matchesSearch).sort(taskSort);
  const filteredAnnouncements = announcements
    .filter(filterFn)
    .filter(matchesSearch)
    .sort((a,b)=>(b.updatedAt || "").localeCompare(a.updatedAt || ""));

  const chips = `
    <div class="chips task-chips">
      <div class="chip ${filter==="активні"?"active":""}" data-action="setTaskFilter" data-arg1="активні"><span class="chip-ico">📌</span><span class="chip-text">Активні</span></div>
      <div class="chip ${filter==="очікує_підтвердження"?"active":""}" data-action="setTaskFilter" data-arg1="очікує_підтвердження"><span class="chip-ico">🟣</span><span class="chip-text">Очікує підтвердження</span></div>
      <div class="chip ${filter==="прострочені"?"active":""}" data-action="setTaskFilter" data-arg1="прострочені"><span class="chip-ico">🟠</span><span class="chip-text">Прострочені</span></div>
      <div class="chip ${filter==="блокери"?"active":""}" data-action="setTaskFilter" data-arg1="блокери"><span class="chip-ico">⛔</span><span class="chip-text">Блокери</span></div>
      <div class="chip ${filter==="без_оновлень"?"active":""}" data-action="setTaskFilter" data-arg1="без_оновлень"><span class="chip-ico">⏳</span><span class="chip-text">Без оновлень</span></div>
      <div class="chip ${filter==="закриті"?"active":""}" data-action="setTaskFilter" data-arg1="закриті"><span class="chip-ico">✅</span><span class="chip-text">Закриті</span></div>
    </div>
  `;
  const personalChips = showAnnouncementsScope ? `
    <div class="chips task-chips personal-chips">
      <div class="chip ${personalFilter==="all"?"active":""}" data-action="setTaskPersonalFilter" data-arg1="all">Все</div>
      <div class="chip ${personalFilter==="tasks"?"active":""}" data-action="setTaskPersonalFilter" data-arg1="tasks">Задачі</div>
      <div class="chip ${personalFilter==="announcements"?"active":""}" data-action="setTaskPersonalFilter" data-arg1="announcements">Оголошення</div>
    </div>
  ` : ``;
  const deptChips = (u.role==="boss") ? `
    <div class="chips dept-chips task-chips">
      ${STATE.departments.map(d=>{
        const c = getVisibleTasksForUser(u).filter(t=>t.departmentId===d.id && t.type!=="personal").length;
        const active = deptFilter===d.id ? "active" : "";
        return `<div class="chip ${active}" data-action="setTaskDeptFilter" data-arg1="${d.id}">${deptBadgeHtml(d)} <span class="mono">${c}</span></div>`;
      }).join("")}
    </div>
  ` : ``;
  const searchUi = `
    <div class="field search-inline">
      <label>Пошук задач / оголошень</label>
      <div class="row" style="gap:8px;">
        <input id="taskSearchInput" type="text" value="${htmlesc(UI.taskSearch)}" placeholder="" data-change="setTaskSearchFromInput" />
        ${UI.taskSearch ? `<button class="btn ghost" data-action="clearTaskSearch">Скинути</button>` : ``}
      </div>
    </div>
  `;
  const showTasks = effectivePersonalFilter!=="announcements";
  const showAnns = showAnnouncementsScope && effectivePersonalFilter!=="tasks";
  const shownCount = (showTasks ? filtered.length : 0) + (showAnns ? filteredAnnouncements.length : 0);
  const totalCount = (showTasks ? tasks.length : 0) + (showAnns ? announcements.length : 0);
  const searchHint = `<div class="hint">Показано: <span class="mono">${shownCount}</span> із <span class="mono">${totalCount}</span></div>`;
  const announcementBtn = (u.role==="boss" && showAnnouncementsScope)
    ? `<button class="btn ghost" data-action="openCreateAnnouncement">📣 Оголошення</button>`
    : ``;
  const buildDeptIndexMap = (list)=>{
    const map = {};
    const counts = {};
    list.forEach(t=>{
      const key = t.departmentId || "personal";
      counts[key] = (counts[key] || 0) + 1;
      map[t.id] = counts[key];
    });
    return map;
  };
  UI.taskIndexMap = buildDeptIndexMap(filtered);

  const showDoneToggle = (filter === "активні");
  const completed = showDoneToggle
    ? tasks.filter(t=>t.status==="закрито").filter(matchesSearch).sort(taskSort)
    : [];
  const completedByDept = {};
  if(showDoneToggle && completed.length){
    completed.forEach(t=>{
      const key = deptLabel(t);
      if(!completedByDept[key]) completedByDept[key] = [];
      completedByDept[key].push(t);
    });
  }

  const isScopeAll = (u.role==="boss" && deptFilter==="all");
  const renderTaskItem = (t, idx)=>{
    const titleTypeClass = (t.type==="managerial")
      ? "task-title-type-managerial"
      : (t.type==="internal")
        ? "task-title-type-internal"
        : "task-title-type-personal";

    const numbering = `${idx + 1}.`;
    const deptName = t.departmentId ? (getDeptById(t.departmentId)?.name || "Відділ") : "Особисто";
    const respName = getUserById(t.responsibleUserId)?.name || "—";
    const titleHtml = highlightMatch(t.title || "");
    const isAnn = isAnnouncement(t);
    const annLabel = isAnn ? announcementAudienceLabel(t.audience) : "";
    const searchMeta = taskSearch
      ? `<div class="task-search-meta">ID: <span class="mono">${highlightMatch(t.id)}</span> • ${highlightMatch(deptName)} • ${highlightMatch(respName)}${isAnn ? ` • ${highlightMatch(annLabel)}` : ""}</div>`
      : "";

    const dueShort = t.dueDate ? dueDisplay(t.dueDate) : "—";
    const statusChip = {cls: statusBadgeClass(t.status), label: statusLabel(t.status), icon: statusIcon(t.status)};
    const prLabel = t.priority || "—";
    const prIcon = priorityIcon(t.priority);
    const prHot = (t.priority==="високий" || t.priority==="терміново");
    const ctrl = controlMeta(t);
    const dueHot = !!t.dueDate && prHot;
    const blocker = (t.status==="блокер" || t.status==="очікування") ? lastBlockerUpdate(t) : null;
    const blockerNote = blocker?.note ? htmlesc(blocker.note).slice(0,120) : "";
    const isLate = isOverdue(t);
    const isDueTodayTask = isDueToday(t) && !isLate;
    const isDone = t.status==="закрито";
    const hideStatus = isAnn || isDone || (t.status==="в_процесі" && !t.dueDate && (t.controlAlways || t.nextControlDate));
    const desc = (t.description || "").trim();
    const descLabel = isAnn ? "Текст" : "Опис";
    const descHtml = (!isAnn && desc) ? `<div class="task-desc">${descLabel}: ${htmlesc(desc)}</div>` : "";
    const closeUpd = isDone ? getCloseUpdate(t) : null;
    const closeAt = isDone ? (closeUpd?.at || t.updatedAt || "") : "";
    const closeShort = isDone ? closeDisplay(closeAt) : "";
    const closeHint = isDone ? closeTitle(closeAt) : "";
    const closeNote = isDone ? normalizeCloseNote(closeUpd?.note || "") : "";
    const resultHtml = (!isAnn && isDone) ? `<div class="task-result">Результат:${closeNote ? htmlesc(closeNote) : "—"}</div>` : "";

    const ctrlClass = t.controlAlways ? "ctrl-always" : (t.nextControlDate ? "ctrl-date" : "");
    return `
      <div class="item task-item ${isAnn ? "announcement-item" : ""} ${t.dueDate ? "has-due" : "no-due"} ${ctrlClass} ${isDueTodayTask ? "due-today" : ""} ${isLate ? "is-overdue" : ""} ${isDone ? "is-completed" : ""}" data-type="${t.type}" data-task-id="${t.id}">
        <div class="row" data-action="openTask" data-arg1="${t.id}">
          <div>
            <div class="task-line">
              <div class="task-title">
                <div class="name ${titleTypeClass}"><span class="task-num mono">${numbering}</span> ${titleHtml}</div>
                ${descHtml}
                ${resultHtml}
                ${searchMeta}
                ${blockerNote ? `<div class="task-note">⛔ ${blockerNote}</div>` : ``}
              </div>
              <div class="task-meta">
                ${!hideStatus ? `<span class="task-token token-status ${statusChip.cls} compact-hide" title="Статус"><span class="token-ico">${statusChip.icon}</span><span class="token-text">${htmlesc(statusChip.label)}</span></span>` : ``}
                ${
                  isDone
                    ? `<span class="task-token token-due token-closed" title="${htmlesc(closeHint)}"><span class="token-ico">✅</span><span class="token-text">${htmlesc(closeShort || "—")}</span></span>`
                    : (t.dueDate
                      ? `<span class="task-token token-due ${dueHot ? "due-hot" : ""}" title="Дедлайн ${dueTitle(t.dueDate)}"><span class="token-ico">⏱</span><span class="token-text">${dueShort}</span></span>`
                    : (ctrl.label
                      ? `<span class="task-token token-due" title="${ctrl.title}"><span class="token-ico">${ctrl.label==="постійно" ? "🎯" : "🗓"}</span><span class="token-text">${htmlesc(ctrl.label)}</span></span>`
                      : ``)
                    )
                }
                ${isAnn ? `` : `<span class="task-token token-priority ${prHot ? "priority-hot" : ""} compact-hide" title="Пріоритет"><span class="token-ico">${prIcon}</span><span class="token-text">${htmlesc(prLabel)}</span></span>`}
              </div>
            </div>
          </div>
        </div>
      </div>
    `;
  };

  const renderDoneToggle = (items, startIdx=0)=>{
    if(!items || !items.length) return "";
    const rows = items.map((t,i)=>renderTaskItem(t, startIdx + i)).join("");
    return `
      <details class="done-toggle">
        <summary>Задачі які виконані <span class="mono">${items.length}</span></summary>
        <div class="done-list">${rows}</div>
      </details>
    `;
  };

  const renderGroupedList = (items)=>{
    let current = null;
    let groupItems = [];
    let counts = null;
    let groupHtml = [];
    let idx = 0;
    const openAttr = taskSearch ? " open" : "";
    const countBucket = (t)=>{
      if(t.dueDate) return "due";
      if(["блокер","очікування"].includes(t.status)) return "blocker";
      if(t.nextControlDate) return "controlDate";
      if(t.controlAlways) return "controlAlways";
      return "other";
    };
    const countBadge = (icon, label, count, cls)=>`
      <span class="dept-count ${cls} ${count ? "" : "zero"}" title="${label}">
        ${icon} <span class="mono">${count}</span>
      </span>
    `;
    const flush = ()=>{
      if(current === null) return;
      const doneItems = showDoneToggle ? (completedByDept[current] || []) : [];
      const doneBlock = doneItems.length ? renderDoneToggle(doneItems, groupItems.length) : "";
      const countsHtml = `
        <span class="dept-counts">
          ${countBadge("⏱", "Дедлайн", counts.due, "count-due")}
          ${countBadge("⛔", "Блокер", counts.blocker, "count-blocker")}
          ${countBadge("🗓", "Контроль з датою", counts.controlDate, "count-ctrl")}
          ${countBadge("🎯", "Контроль постійно", counts.controlAlways, "count-always")}
        </span>
      `;
      groupHtml.push(`
        <details class="dept-group dept-disclosure"${openAttr}>
          <summary class="dept-title">
            <span class="dept-title-text">${highlightMatch(current)}</span>
            ${countsHtml}
          </summary>
          <div class="dept-list">${groupItems.join("")}${doneBlock}</div>
        </details>
      `);
    };
    items.forEach(t=>{
      const label = deptLabel(t);
      if(label !== current){
        flush();
        current = label;
        groupItems = [];
        counts = {due:0, blocker:0, controlDate:0, controlAlways:0};
        idx = 0;
      }
      const bucketKey = countBucket(t);
      if(bucketKey in counts) counts[bucketKey] += 1;
      groupItems.push(renderTaskItem(t, idx));
      idx += 1;
    });
    flush();
    return groupHtml.join("");
  };

  const emptyHint = (() => {
    if(filter==="блокери") return `<div class="hint">Немає блокерів. Якщо є перешкода — постав статус “Блокер”.</div>`;
    if(filter==="прострочені") return `<div class="hint">Немає прострочених задач.</div>`;
    if(filter==="очікує_підтвердження") return `<div class="hint">Немає задач на підтвердження.</div>`;
    if(filter==="без_оновлень") return `<div class="hint">Немає задач без оновлень &gt; 7 днів.</div>`;
    if(filter==="закриті") return `<div class="hint">Немає закритих задач за цим фільтром.</div>`;
    return `<div class="hint">Немає задач за цим фільтром.</div>`;
  })();
  let tasksList = "";
  if(filtered.length){
    if(isScopeAll){
      tasksList = renderGroupedList(filtered);
    } else {
      const doneBlock = showDoneToggle ? renderDoneToggle(completed, filtered.length) : "";
      tasksList = filtered.map(renderTaskItem).join("") + doneBlock;
    }
  } else if(showDoneToggle && completed.length){
    const doneBlock = renderDoneToggle(completed, 0);
    tasksList = `<div class="hint">Немає активних задач.</div>${doneBlock}`;
  } else {
    tasksList = emptyHint;
  }

  const canSeeMeetingAnnouncements = (u.role==="boss") || isDeptHeadLike;
  const staffAnnouncements = filteredAnnouncements.filter(t=>t.audience !== "meeting");
  const meetingAnnouncements = filteredAnnouncements.filter(t=>t.audience === "meeting");
  const renderAnnouncementSection = (title, list)=>`
    <div class="announcement-section">
      <div class="announcement-title">${title} <span class="mono">${list.length}</span></div>
      <div class="announcement-list">
        ${list.length ? list.map(renderTaskItem).join("") : `<div class="hint">Немає оголошень.</div>`}
      </div>
    </div>
  `;
  const announcementsBlock = showAnnouncementsScope ? `
    <div class="announcement-block">
      ${renderAnnouncementSection("Оголошення для особового складу", staffAnnouncements)}
      ${canSeeMeetingAnnouncements ? renderAnnouncementSection("Оголошення для наради", meetingAnnouncements) : ``}
    </div>
  ` : "";

  const listParts = [];
  if(showAnnouncementsScope && effectivePersonalFilter!=="tasks"){
    listParts.push(announcementsBlock);
  }
  if(showAnnouncementsScope && effectivePersonalFilter==="all" && announcementsBlock){
    listParts.push(`<div class="section-title">Задачі</div>`);
  }
  if(effectivePersonalFilter!=="announcements"){
    listParts.push(tasksList);
  }
  const list = listParts.join("");

  const body = `
    <div class="card">
      <div class="card-h">
        <div class="t">Задачі</div>
        <div style="display:flex;align-items:center;gap:8px;">
          ${u.role==="boss" ? `<button class="btn ghost" data-action="openTasksExportDialog">⬇️ Excel</button>` : ``}
          ${announcementBtn}
          ${u.role==="boss" ? `<button class="btn ghost" data-action="toggleTaskScope">${UI.taskDeptFilter==="personal" ? "Мої" : "Всі"}</button>` : `<span class="badge b-blue">Мій відділ</span>`}
        </div>
      </div>
      <div class="card-b">
        <div class="task-toolbar">
          ${chips}
          ${personalChips}
          ${searchUi}
        </div>
        ${deptChips}
        ${searchHint}
        <div class="list">${list}</div>
      </div>
    </div>
  `;

  const tabs = (u.role==="boss")
    ? [
      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},
      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},
      {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},
    ]
    : [
      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},
      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},
    ];

  const subtitle = (u.role==="boss") ? "Керівник" : (getDeptById(u.departmentId)?.name ?? "Відділ");
  const fabAction = ()=>{
    if(u.role==="boss"){
      showSheet("Додати", `
        <div class="actions">
          <button class="btn primary" data-action="hideThen" data-next="openCreateTask" data-arg1="personal">➕ Моя задача</button>
          <button class="btn ghost" data-action="hideThen" data-next="openCreateTask" data-arg1="managerial">➕ Управлінська</button>
        </div>
        <div class="sep"></div>
        <button class="btn ghost" data-action="hideSheet">Закрити</button>
      `);
    } else {
      openCreateTask('internal');
    }
  };

  appShell({title:"Задачі", subtitle, bodyHtml: body, showFab:true, fabAction, tabs});

  const fab = document.getElementById("fab");
  if(fab){
    document.querySelectorAll(".dept-disclosure > summary").forEach((el)=>{
      el.addEventListener("dblclick", (e)=>{
        if(e.button !== 0) return;
        e.preventDefault();
        e.stopPropagation();
        fab.click();
      });
    });
    document.querySelectorAll(".dept-chips .chip").forEach((el)=>{
      el.addEventListener("click", (e)=>{
        if(e.button !== 0) return;
        fab.click();
      });
    });
  }
}

function quickActionsForTask(u, t){
  const {isDeptHeadLike} = asDeptRole(u);
  const isBoss = (u.role==="boss");
  const canUpdate = isBoss || isDeptHeadLike;
  if(!canUpdate || t.status==="закрито") return "";

  const isAnn = isAnnouncement(t);
  if(isAnn){
    if(!isBoss) return "";
    const btns = [];
    btns.push(`<button class="btn ok" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="закрито">✅ Виконано</button>`);
    if(canEditTask(u, t)){
      btns.push(`<button class="btn ghost" data-action="openEditTask" data-arg1="${t.id}">✏️ Редагувати</button>`);
    }
    return `<div class="actions">${btns.join("")}</div>`;
  }

  const btns = [];
  const isBlocked = (t.status==="блокер" || t.status==="очікування");
  const blockerBtn = isBlocked
    ? `<button class="btn warn" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="в_процесі">🔓 Розблок</button>`
    : `<button class="btn warn" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="блокер">⛔ Блокер</button>`;

  if(isBoss){
    if(t.type==="managerial" && t.status==="очікує_підтвердження"){
      btns.push(`<button class="btn ok" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="закрито">✅ Підтвердити</button>`);
    } else {
      btns.push(`<button class="btn ok" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="закрито">✅ Закрити</button>`);
    }
    btns.push(blockerBtn);
  } else {
    if(t.type==="internal"){
      btns.push(`<button class="btn ok" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="закрито">✅ Закрити</button>`);
    }
    if(t.type==="managerial"){
      btns.push(`<button class="btn violet" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="очікує_підтвердження">🟣 Запит закриття</button>`);
    }
    btns.push(blockerBtn);
  }

  if(canEditTask(u, t)){
    btns.push(`<button class="btn ghost" data-action="openEditTask" data-arg1="${t.id}">✏️ Редагувати</button>`);
  }
  return `<div class="actions">${btns.join("")}</div>`;
}

function openStatusReasonModal(taskId, status){
  const t = STATE.tasks.find(x=>x.id===taskId);
  if(!t) return;
  const isBlocking = status === "блокер";
  const isClosing = status === "закрито";
  const title = isBlocking
    ? "Блокер: вкажи причину"
    : (isClosing ? "Закриття: результат" : "Розблокування: причина");
  const label = isBlocking
    ? "Причина блокера"
    : (isClosing ? "Результат / причина закриття" : "Причина розблокування");
  const hint = isBlocking
    ? "Опиши, що заважає або кого/чого очікуємо."
    : (isClosing ? "Коротко: що зроблено або який результат." : "Що змінилося і чому можна рухатись далі.");
  const placeholder = isBlocking
    ? "Наприклад: немає доступу / чекаємо підтвердження / бракує ресурсу."
    : (isClosing ? "Наприклад: виконано повністю / передано результат / підтверджено." : "Наприклад: отримали доступ / підтвердили рішення / ресурс з’явився.");

  showSheet(title, `
    <div class="hint">${hint}</div>
    <div class="field">
      <label>${label}</label>
      <textarea id="statusReason" placeholder="${placeholder}"></textarea>
    </div>
    <div class="actions" style="margin-top:14px;">
      <button class="btn primary" data-action="submitStatusReason" data-arg1="${t.id}" data-arg2="${status}">Зберегти</button>
      <button class="btn ghost" data-action="hideSheet">Скасувати</button>
    </div>
  `);
}

function openEditAnnouncement(taskId){
  const u = currentSessionUser();
  const t = STATE.tasks.find(x=>x.id===taskId);
  if(!u || !t) return;
  if(u.role!=="boss"){
    showSheet("Немає прав", `<div class="hint">Оголошення може редагувати лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  showSheet("Редагувати оголошення", `
    <div class="hint">Оголошення: <b>${htmlesc(t.id)}</b></div>
    <div class="field">
      <label>Аудиторія</label>
      <select id="aAudience">
        <option value="staff" ${t.audience==="staff" ? "selected" : ""}>Особовий склад</option>
        <option value="meeting" ${t.audience==="meeting" ? "selected" : ""}>Нарада (керівництво)</option>
      </select>
    </div>
    <div class="field">
      <label>Заголовок</label>
      <input id="aTitle" value="${htmlesc(t.title)}" />
    </div>
    <div class="actions" style="margin-top:14px;">
      <button class="btn primary" data-action="saveAnnouncementEdits" data-arg1="${t.id}">Зберегти</button>
      <button class="btn ghost" data-action="hideSheet">Скасувати</button>
    </div>
  `);
}

function appendReportText(id, text){
  if(!text) return;
  const el = document.getElementById(id);
  if(!el) return;
  const cur = el.value.trim();
  el.value = cur ? `${cur}\n${text}` : text;
}
function fillReportIfEmpty(id, text){
  if(!text) return;
  const el = document.getElementById(id);
  if(!el) return;
  if(!el.value.trim()) el.value = text;
}
function buildReportTemplate(){
  return {
    done: "• [задача/подія] — результат",
    progress: "• [задача] — поточний стан / що залишилось",
    blocked: "• [задача] — причина / кого чекаємо",
  };
}
function applyReportTemplate(){
  const t = buildReportTemplate();
  fillReportIfEmpty("rDone", t.done);
  fillReportIfEmpty("rProg", t.progress);
  fillReportIfEmpty("rBlock", t.blocked);
  showToast("Шаблон вставлено в порожні поля.", "info");
}
function buildAutoReport(){
  const u = currentSessionUser();
  if(!u) return {done:"", progress:"", blocked:"", empty:true};
  const today = kyivDateStr();
  const updates = STATE.taskUpdates
    .filter(x=>x.authorUserId===u.id && toDateOnly(x.at)===today)
    .sort((a,b)=>b.at.localeCompare(a.at));

  if(!updates.length) return {done:"", progress:"", blocked:"", empty:true};

  const latestByTask = {};
  updates.forEach(upd=>{
    if(!latestByTask[upd.taskId]) latestByTask[upd.taskId] = upd;
  });

  const done = [];
  const progress = [];
  const blocked = [];

  Object.values(latestByTask).forEach(upd=>{
    const task = STATE.tasks.find(t=>t.id===upd.taskId);
    const label = task ? `${task.id} — ${task.title}` : `Задача ${upd.taskId}`;
    const note = upd.note ? `: ${upd.note}` : "";
    const line = `• ${label}${note}`;
    if(upd.status==="закрито"){
      done.push(line);
    } else if(upd.status==="блокер" || upd.status==="очікування"){
      blocked.push(line);
    } else {
      progress.push(line);
    }
  });

  return {
    done: done.join("\n"),
    progress: progress.join("\n"),
    blocked: blocked.join("\n"),
    empty: false
  };
}
function autoFillReport(){
  const data = buildAutoReport();
  if(data.empty){
    showToast("Сьогодні немає оновлень задач для автозаповнення.", "info");
    return;
  }
  appendReportText("rDone", data.done);
  appendReportText("rProg", data.progress);
  appendReportText("rBlock", data.blocked);
  showToast("Звіт автозаповнено з оновлень задач.", "ok");
}

function submitStatusReason(taskId, status){
  const u = currentSessionUser();
  const t = STATE.tasks.find(x=>x.id===taskId);
  if(!u || !t) return;

  const reason = (document.getElementById("statusReason")?.value || "").trim();
  if(reason.length < 3){
    showToast("Вкажи причину (мін. 3 символи).", "warn");
    return;
  }

  const {isDeptHeadLike} = asDeptRole(u);
  const isBoss = (u.role==="boss");
  if(isAnnouncement(t) && !isBoss){
    showSheet("Немає прав", `<div class="hint">Оголошення може змінювати лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  if(!(isBoss || isDeptHeadLike)){
    showSheet("Немає прав", `<div class="hint">Тільки начальник відділу (або в.о.) може змінювати статуси.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  if(!isBoss && t.departmentId !== u.departmentId){
    showSheet("Немає прав", `<div class="hint">Ви не маєте доступу до іншого відділу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  if(!isBoss && t.type==="managerial" && status==="закрито"){
    showSheet("Обмеження", `<div class="hint">Управлінську задачу закриває тільки керівник. Використайте “Запит закриття”.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const wasBlocked = (t.status==="блокер" || t.status==="очікування");
  const isBlocking = (status === "блокер");
  const stillBlocked = (status==="блокер" || status==="очікування");
  let note = `Статус → ${statusLabel(status)}: ${reason}`;
  if(isBlocking){
    note = `Блокер: ${reason}`;
  } else if(status==="закрито"){
    note = reason;
  } else if(wasBlocked && !stillBlocked){
    note = `Розблоковано → ${statusLabel(status)}: ${reason}`;
  }

  updateTask(taskId, {status}, u.id, note);
  hideSheet();
  render();
  showToast(`Статус оновлено: ${statusLabel(status)}`, "ok");
}

function setTaskStatus(taskId, status, bypassConfirm=false){
  const u = currentSessionUser();
  const t = STATE.tasks.find(x=>x.id===taskId);
  if(!t) return;

  const {isDeptHeadLike} = asDeptRole(u);
  if(isAnnouncement(t) && u.role!=="boss"){
    showSheet("Немає прав", `<div class="hint">Оголошення може змінювати лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  if(isAnnouncement(t) && status==="закрито"){
    updateTask(taskId, {status}, u.id, "Оголошення виконано");
    render();
    showToast(`Статус оновлено: ${statusLabel(status)}`, "ok");
    return;
  }

  if(!(u.role==="boss" || isDeptHeadLike)){
    showSheet("Немає прав", `<div class="hint">Тільки начальник відділу (або в.о.) може змінювати статуси.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  if(u.role!=="boss" && t.departmentId !== u.departmentId){
    showSheet("Немає прав", `<div class="hint">Ви не маєте доступу до іншого відділу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  if(u.role!=="boss" && t.type==="managerial" && status==="закрито"){
    showSheet("Обмеження", `<div class="hint">Управлінську задачу закриває тільки керівник. Використайте “Запит закриття”.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  if(status==="закрито"){
    return openStatusReasonModal(taskId, status);
  }

  const isBlocking = (status === "блокер");
  const isUnblocking = (t.status==="блокер" || t.status==="очікування") && !(status==="блокер" || status==="очікування");
  if(isBlocking || isUnblocking){
    return openStatusReasonModal(taskId, status);
  }

  updateTask(taskId, {status}, u.id, `Статус → ${statusLabel(status)}`);
  render();
  showToast(`Статус оновлено: ${statusLabel(status)}`, "ok");
}

function confirmTaskClose(taskId){
  setTaskStatus(taskId, "закрито", true);
}

function setControlDate(taskId){
  const u = currentSessionUser();
  const t = STATE.tasks.find(x=>x.id===taskId);
  if(!t) return;

  const {isDeptHeadLike} = asDeptRole(u);
  if(!(u.role==="boss" || isDeptHeadLike)){
    showSheet("Немає прав", `<div class="hint">Тільки керівник або начальник відділу (в.о.) може змінювати контрольну дату.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  if(t.dueDate){
    showSheet("Контроль недоступний", `<div class="hint">Контроль недоступний.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const isAlways = !!t.controlAlways;
  showSheet("Контрольна дата", `
    <div class="hint">Контроль — коли потрібно повернутися до задачі (або постійний контроль).</div>
    <div class="field">
      <label>Контроль</label>
      <input id="ctrlDate" type="date" value="${isAlways ? "" : (t.nextControlDate ?? kyivDateStr())}" ${isAlways ? "disabled" : ""} />
    </div>
    <div class="field">
      <label><input id="ctrlAlways" type="checkbox" data-change="toggleCtrlAlways" ${isAlways ? "checked" : ""} /> Постійний контроль (без дати)</label>
    </div>
    <div class="field">
      <label><input id="ctrlClear" type="checkbox" /> Прибрати контрольну дату</label>
    </div>
    <div class="actions" style="margin-top:14px;">
      <button class="btn primary" data-action="applyControlDate" data-arg1="${t.id}">Зберегти</button>
      <button class="btn ghost" data-action="hideSheet">Скасувати</button>
    </div>
  `);
}

function applyControlDate(taskId){
  const u = currentSessionUser();
  const clear = document.getElementById("ctrlClear")?.checked;
  const always = document.getElementById("ctrlAlways")?.checked;
  const d = (document.getElementById("ctrlDate")?.value || null);

  let nextControlDate = null;
  let controlAlways = false;
  let note = "Контроль → без контролю";
  let toast = "Контроль прибрано";

  if(!clear){
    if(always){
      controlAlways = true;
      note = "Контроль → постійно";
      toast = "Контроль: постійно";
    } else if(d){
      nextControlDate = d;
      note = `Контроль → ${fmtDate(d)}`;
      toast = `Контроль: ${fmtDate(d)}`;
    }
  }

  updateTask(taskId, {nextControlDate, controlAlways}, u.id, note);
  hideSheet();
  render();
  showToast(toast, "info");
}

function openTask(taskId){
  const u = currentSessionUser();
  const t = STATE.tasks.find(x=>x.id===taskId);
  if(!t) return;

  if(u.role!=="boss" && t.departmentId && !canAccessDept(u, t.departmentId)){
    showSheet("Немає доступу", `<div class="hint">Ця задача належить іншому відділу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  if(t.type==="personal" && u.role!=="boss" && !isAnnouncement(t)){
    showSheet("Немає доступу", `<div class="hint">Особисті задачі бачить тільки керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  if(isAnnouncement(t) && !canSeeAnnouncement(u, t)){
    showSheet("Немає доступу", `<div class="hint">Це оголошення не призначене для вашої ролі.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const dept = t.departmentId ? getDeptById(t.departmentId) : null;
  const resp = getUserById(t.responsibleUserId);
  const upd = STATE.taskUpdates.filter(x=>x.taskId===t.id).sort((a,b)=>b.at.localeCompare(a.at)).slice(0,8);
  const ctrl = controlMeta(t);
  const prHot = (t.priority==="високий" || t.priority==="терміново");
  const dueHot = !!t.dueDate && prHot;
  const isDone = t.status==="закрито";
  const isAnn = isAnnouncement(t);
  const annLabel = isAnn ? announcementAudienceLabel(t.audience) : "";
  const descLabel = isAnn ? "Текст" : "Опис";
  const statusChip = {cls: statusBadgeClass(t.status), label: statusLabel(t.status), icon: statusIcon(t.status)};
  const hideStatus = isAnn || isDone || (t.status==="в_процесі" && !t.dueDate && (t.controlAlways || t.nextControlDate));
  const closeUpd = isDone ? getCloseUpdate(t) : null;
  const closeAt = isDone ? (closeUpd?.at || t.updatedAt || "") : "";
  const closeShort = isDone ? closeDisplay(closeAt) : "";
  const closeHint = isDone ? closeTitle(closeAt) : "";
  const closeNote = isDone ? normalizeCloseNote(closeUpd?.note || "") : "";
  const titleTypeClass = (t.type==="managerial")
    ? "task-title-type-managerial"
    : (t.type==="internal")
      ? "task-title-type-internal"
      : "task-title-type-personal";
  const titleClass = isDueToday(t) ? "task-title-due-today" : (t.dueDate ? "task-title-due" : titleTypeClass);
  let deptNum = UI.taskIndexMap?.[t.id];
  if(!deptNum){
    const key = t.departmentId || "personal";
    const list = getVisibleTasksForUser(u).filter(x=>(x.departmentId || "personal")===key);
    const bucket = (x)=>{
      if(x.dueDate) return 0;
      if(["блокер","очікування"].includes(x.status)) return 1;
      if(x.nextControlDate) return 2;
      if(x.controlAlways) return 3;
      return 4;
    };
    const dateKey = (x)=>{
      if(x.dueDate) return dueSortKey(x.dueDate);
      if(x.nextControlDate) return x.nextControlDate;
      if(x.controlAlways) return "0000-00-00";
      return "9999-99-99";
    };
    list.sort((a,b)=>{
      const ba = bucket(a);
      const bb = bucket(b);
      if(ba!==bb) return ba - bb;
      const dka = dateKey(a);
      const dkb = dateKey(b);
      if(dka!==dkb) return dka.localeCompare(dkb);
      return (a.title || "").localeCompare(b.title || "");
    });
    const idx = list.findIndex(x=>x.id===t.id);
    deptNum = idx>=0 ? idx+1 : null;
  }

  showSheet(isAnn ? "Оголошення" : "Картка задачі", `
    <div class="item task-sheet-compact" style="cursor:default;">
      <div class="task-line">
        <div class="task-title">
          <div class="name ${titleClass}">${deptNum ? `<span class="task-num mono">${deptNum}.</span>` : ""} ${htmlesc(t.title)}</div>
        </div>
        <div class="task-meta">
          ${!hideStatus ? `<span class="task-token token-status ${statusChip.cls} compact-hide" title="Статус"><span class="token-ico">${statusChip.icon}</span><span class="token-text">${htmlesc(statusChip.label)}</span></span>` : ``}
          ${
            isDone
              ? `<span class="task-token token-due token-closed" title="${htmlesc(closeHint)}"><span class="token-ico">✅</span><span class="token-text">${htmlesc(closeShort || "—")}</span></span>`
              : (t.dueDate
                ? `<span class="task-token token-due ${dueHot ? "due-hot" : ""}" title="Дедлайн ${dueTitle(t.dueDate)}"><span class="token-ico">⏱</span><span class="token-text">${t.dueDate ? dueDisplay(t.dueDate) : "—"}</span></span>`
              : (ctrl.label
                ? `<span class="task-token token-due" title="${ctrl.title}"><span class="token-ico">${ctrl.label==="постійно" ? "🎯" : "🗓"}</span><span class="token-text">${htmlesc(ctrl.label)}</span></span>`
                : ``)
              )
          }
          ${isAnn ? `` : `<span class="task-token token-priority ${prHot ? "priority-hot" : ""} compact-hide"><span class="token-ico">${priorityIcon(t.priority)}</span><span class="token-text">${htmlesc(t.priority)}</span></span>`}
        </div>
      </div>

      ${isAnn ? `` : `<div class="hint"><b>${descLabel}:</b> ${t.description ? htmlesc(t.description) : "—"}</div>`}
      ${(!isAnn && isDone) ? `<div class="hint"><b>Результат:</b>${closeNote ? htmlesc(closeNote) : "—"}</div>` : ``}

      <details class="task-disclosure" ${upd.length ? "" : "open"}>
        <summary>Оновлення (${upd.length})</summary>
        <div class="hint">
          ${upd.length ? upd.map(x=>{
            const au = getUserById(x.authorUserId);
            const who = au ? `${au.name}${isActingHead(au.id) ? " (в.о.)" : ""}` : "—";
            return `• <span class="mono">${htmlesc(x.at)}</span> — <b>${htmlesc(who)}</b>: ${htmlesc(x.note || "")}`;
          }).join("<br/>") : "Немає оновлень."}
        </div>
      </details>
    </div>
    <div class="sep"></div>
    ${
      isDone
        ? `<button class="btn primary" data-action="hideSheet">OK</button>`
        : (quickActionsForTask(u, t) || `<button class="btn primary" data-action="hideSheet">Закрити</button>`)
    }
  `);
}

function openEditTask(taskId){
  const u = currentSessionUser();
  const t = STATE.tasks.find(x=>x.id===taskId);
  if(!u || !t) return;
  if(isAnnouncement(t)) return openEditAnnouncement(taskId);

  if(!canEditTask(u, t)){
    showSheet("Немає прав", `<div class="hint">Ви не маєте прав редагувати цю задачу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const isBoss = (u.role==="boss");
  const isPersonal = (t.type==="personal");
  const today = kyivDateStr();

  const deptOptions = isPersonal
    ? []
    : (isBoss ? STATE.departments : STATE.departments.filter(d=>d.id===u.departmentId));

  createTaskUserOptions = (deptId)=>{
    if(isPersonal) return [STATE.users.find(x=>x.id==="u_boss")].filter(Boolean);
    return getDeptResponsibleOptions(deptId);
  };

  const deptId = t.departmentId || (deptOptions[0]?.id ?? "");
  const noDue = !t.dueDate;
  const dueParts = splitDateTime(t.dueDate);

  showSheet("Редагувати задачу", `
    <div class="hint">
      Редагування задачі: <b>${htmlesc(t.id)}</b>
    </div>

    <div class="field">
      <label>Назва</label>
      <input id="tTitle" value="${htmlesc(t.title)}" />
    </div>

    <div class="field">
      <label>Опис (опційно)</label>
      <textarea id="tDesc">${htmlesc(t.description || "")}</textarea>
    </div>

    ${!isPersonal ? `
      <div class="field">
        <label>Відділ</label>
        <select id="tDept" data-change="refreshRespOptions" ${isBoss ? "" : "disabled"}>
          ${deptOptions.map(d=>`<option value="${d.id}" ${d.id===deptId ? "selected" : ""}>${htmlesc(d.name)}</option>`).join("")}
        </select>
      </div>

      <div class="field">
        <label>Відповідальний</label>
        <select id="tResp"></select>
      </div>
    ` : `
      <div class="field">
        <label>Відповідальний</label>
        <input value="Керівник (ви)" disabled />
      </div>
    `}

    <div class="row2">
      <div class="field">
        <label>Пріоритет</label>
        <select id="tPr">
          <option value="звичайний" ${t.priority==="звичайний" ? "selected" : ""}>Звичайний</option>
          <option value="високий" ${t.priority==="високий" ? "selected" : ""}>Високий</option>
          <option value="терміново" ${t.priority==="терміново" ? "selected" : ""}>Терміново</option>
          <option value="низький" ${t.priority==="низький" ? "selected" : ""}>Низький</option>
        </select>
      </div>

      <div class="field">
        <label>Дедлайн</label>
        <div class="row" style="display:flex;gap:8px;">
          <input id="tDue" type="date" value="${dueParts.date}" />
          <input id="tDueTime" type="time" value="${dueParts.time}" />
        </div>
      </div>
    </div>

    <div class="field">
      <label><input id="noDue" type="checkbox" data-change="toggleNoDue" ${noDue ? "checked" : ""} /> Без дедлайну</label>
    </div>

    <div id="ctrlBlock">
      <div class="field">
        <label>Контрольна дата</label>
        <input id="tCtrl" type="date" value="${t.nextControlDate ?? ""}" />
      </div>
      <div class="field">
        <label><input id="tCtrlAlways" type="checkbox" data-change="toggleCtrlAlways" ${t.controlAlways ? "checked" : ""} /> Постійний контроль (без дати)</label>
      </div>
      <div class="hint">Контроль використовується тільки якщо немає дедлайну.</div>
    </div>

    <div class="actions" style="margin-top:14px;">
      <button class="btn primary" data-action="saveTaskEdits" data-arg1="${t.id}">Зберегти</button>
      <button class="btn ghost" data-action="hideSheet">Скасувати</button>
    </div>
  `);

  toggleNoDue();
  if(!isPersonal) refreshRespOptions();
  const respSel = document.getElementById("tResp");
  if(respSel && t.responsibleUserId){
    respSel.value = t.responsibleUserId;
  }
}

function saveTaskEdits(taskId){
  const u = currentSessionUser();
  const t = STATE.tasks.find(x=>x.id===taskId);
  if(!u || !t) return;

  if(!canEditTask(u, t)){
    showSheet("Немає прав", `<div class="hint">Ви не маєте прав редагувати цю задачу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const title = document.getElementById("tTitle").value.trim();
  if(!title){
    showSheet("Помилка", `<div class="hint">Вкажи назву задачі.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const desc = document.getElementById("tDesc").value.trim();
  const pr = document.getElementById("tPr").value;
  const noDue = document.getElementById("noDue").checked;
  const dueDateVal = document.getElementById("tDue").value || null;
  const dueTimeVal = document.getElementById("tDueTime")?.value || "";
  const due = noDue ? null : joinDateTime(dueDateVal, dueTimeVal);
  const ctrlAlways = noDue ? !!document.getElementById("tCtrlAlways")?.checked : false;
  const ctrl = (noDue && !ctrlAlways) ? (document.getElementById("tCtrl").value || null) : null;

  if(!noDue && !dueDateVal){
    showSheet("Помилка", `<div class="hint">Вкажи дедлайн або вибери “Без дедлайну”.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const isBoss = (u.role==="boss");
  let departmentId = t.departmentId;
  let responsibleUserId = t.responsibleUserId;

  if(t.type!=="personal"){
    const deptSel = document.getElementById("tDept");
    if(isBoss && deptSel) departmentId = deptSel.value;
    if(!departmentId) departmentId = t.departmentId;
    const respSel = document.getElementById("tResp");
    if(respSel) responsibleUserId = respSel.value;

    const allowed = getDeptResponsibleOptions(departmentId).map(x=>x.id);
    if(!allowed.includes(responsibleUserId)){
      responsibleUserId = allowed[0] || responsibleUserId;
    }
  }

  const patch = {
    title,
    description: desc,
    priority: pr,
    dueDate: due,
    nextControlDate: ctrl,
    controlAlways: ctrlAlways,
    departmentId,
    responsibleUserId,
  };

  const oldDept = t.departmentId ? (getDeptById(t.departmentId)?.name || "—") : "Особисто";
  const newDept = departmentId ? (getDeptById(departmentId)?.name || "—") : "Особисто";
  const oldResp = t.responsibleUserId ? (getUserById(t.responsibleUserId)?.name || "—") : "—";
  const newResp = responsibleUserId ? (getUserById(responsibleUserId)?.name || "—") : "—";
  const ctrlLabel = (d, always)=> always ? "постійно" : (d ? fmtDate(d) : "—");
  const changes = [];
  if(title !== t.title) changes.push(`Назва: "${shorten(t.title)}" → "${shorten(title)}"`);
  if(desc !== (t.description || "")) changes.push(`Опис: "${shorten(t.description || "")}" → "${shorten(desc)}"`);
  if(pr !== t.priority) changes.push(`Пріоритет: ${t.priority || "—"} → ${pr || "—"}`);
  if(due !== t.dueDate) changes.push(`Дедлайн: ${t.dueDate ? dueTitle(t.dueDate) : "—"} → ${due ? dueTitle(due) : "—"}`);
  if(ctrlLabel(t.nextControlDate, t.controlAlways) !== ctrlLabel(ctrl, ctrlAlways)) changes.push(`Контроль: ${ctrlLabel(t.nextControlDate, t.controlAlways)} → ${ctrlLabel(ctrl, ctrlAlways)}`);
  if(departmentId !== t.departmentId) changes.push(`Відділ: ${oldDept} → ${newDept}`);
  if(responsibleUserId !== t.responsibleUserId) changes.push(`Відповідальний: ${oldResp} → ${newResp}`);
  const note = changes.length ? `Змінено: ${changes.join("; ")}` : "Редагування без змін";

  updateTask(taskId, patch, u.id, note);
  hideSheet();
  render();
  showToast("Зміни збережено", "ok");
}

function saveAnnouncementEdits(taskId){
  const u = currentSessionUser();
  const t = STATE.tasks.find(x=>x.id===taskId);
  if(!u || !t) return;
  if(u.role!=="boss"){
    showSheet("Немає прав", `<div class="hint">Оголошення може редагувати лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const audience = document.getElementById("aAudience")?.value || "staff";
  const title = (document.getElementById("aTitle")?.value || "").trim();
  if(!title){
    showSheet("Помилка", `<div class="hint">Вкажи заголовок оголошення.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const changes = [];
  if(title !== t.title) changes.push(`Назва: "${shorten(t.title)}" → "${shorten(title)}"`);
  if(audience !== (t.audience || "staff")) changes.push(`Аудиторія: ${announcementAudienceLabel(t.audience)} → ${announcementAudienceLabel(audience)}`);
  const note = changes.length ? `Оголошення: ${changes.join("; ")}` : "Оголошення без змін";

  updateTask(taskId, {title, audience, priority: null}, u.id, note);
  hideSheet();
  render();
  showToast("Оголошення оновлено", "ok");
}

/* ===========================
   CREATE TASK
=========================== */
let createTaskUserOptions = null;

function toggleNoDue(){
  const noDueEl = document.getElementById("noDue");
  const due = document.getElementById("tDue");
  const dueTime = document.getElementById("tDueTime");
  const ctrl = document.getElementById("tCtrl");
  const ctrlAlways = document.getElementById("tCtrlAlways");
  const ctrlBlock = document.getElementById("ctrlBlock");
  if(!noDueEl || !due) return;

  const no = noDueEl.checked;
  due.disabled = no;
  if(dueTime) dueTime.disabled = no;
  if(no){
    due.value = "";
    if(dueTime) dueTime.value = "";
    if(ctrl){
      if(!ctrl.value && !(ctrlAlways && ctrlAlways.checked)){
        ctrl.value = addDays(kyivDateStr(), 1);
      }
      ctrl.disabled = !!(ctrlAlways && ctrlAlways.checked);
    }
    if(ctrlAlways) ctrlAlways.disabled = false;
    if(ctrlBlock) ctrlBlock.classList.remove("disabled");
  } else {
    if(!due.value){
      due.value = addDays(kyivDateStr(), 3);
    }
    if(ctrl){
      ctrl.value = "";
      ctrl.disabled = true;
    }
    if(ctrlAlways){
      ctrlAlways.checked = false;
      ctrlAlways.disabled = true;
    }
    if(ctrlBlock) ctrlBlock.classList.add("disabled");
  }
}
function toggleCtrlAlways(){
  const always = document.getElementById("tCtrlAlways") || document.getElementById("ctrlAlways");
  const ctrl = document.getElementById("tCtrl") || document.getElementById("ctrlDate");
  if(!always || !ctrl) return;

  if(always.checked){
    ctrl.value = "";
    ctrl.disabled = true;
    return;
  }

  ctrl.disabled = false;
  if(!ctrl.value){
    ctrl.value = (ctrl.id === "tCtrl") ? addDays(kyivDateStr(), 1) : kyivDateStr();
  }
}
function refreshRespOptions(){
  const deptSel = document.getElementById("tDept");
  const respSel = document.getElementById("tResp");
  if(!deptSel || !respSel || typeof createTaskUserOptions !== "function") return;

  const opts = createTaskUserOptions(deptSel.value);
  respSel.innerHTML = opts.map(x=>`<option value="${x.id}">${htmlesc(x.name)}</option>`).join("");
}

function openCreateTask(kind){
  const u = currentSessionUser();
  const {isDeptHeadLike} = asDeptRole(u);

  if(kind==="managerial" && u.role!=="boss"){
    showSheet("Немає прав", `<div class="hint">Управлінські задачі створює тільки керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  if(kind==="personal" && u.role!=="boss"){
    showSheet("Немає прав", `<div class="hint">Особисті задачі створює та бачить тільки керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  if(kind==="internal" && !(u.role==="boss" || isDeptHeadLike)){
    showSheet("Немає прав", `<div class="hint">Внутрішні задачі може створювати начальник відділу (або в.о.).</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const today = kyivDateStr();
  const isPersonal = (kind==="personal");

  const deptOptions = isPersonal
    ? []
    : (u.role==="boss" ? STATE.departments : STATE.departments.filter(d=>d.id===u.departmentId));

  const userOptions = (deptId)=>{
    if(isPersonal) return [STATE.users.find(x=>x.id==="u_boss")].filter(Boolean);
    const list = STATE.users.filter(x=>x.active && x.departmentId===deptId && (x.role==="executor" || x.role==="dept_head"));
    return list;
  };
  createTaskUserOptions = userOptions;

  showSheet(
    kind==="managerial" ? "Нова управлінська задача" :
    kind==="internal" ? "Нова внутрішня задача" :
    "Нова моя задача",
    `
    <div class="hint">
      ${
        kind==="managerial" ? "Управлінська: ставить керівник, закриває керівник." :
        kind==="internal" ? "Внутрішня: створює начальник/в.о., закриває начальник/в.о." :
        "Моя задача: для себе (дзвінки/нагадування/контроль)."
      }
      <br/>Контроль доступний лише <b>без дедлайну</b> (можна і <b>постійний контроль</b>).
    </div>

    <div class="field">
      <label>Назва</label>
      <input id="tTitle" placeholder="Коротко: що зробити" />
    </div>

    <div class="field">
      <label>Опис (опційно)</label>
      <textarea id="tDesc" placeholder="Деталі / очікуваний результат"></textarea>
    </div>

    ${!isPersonal ? `
      <div class="field">
        <label>Відділ</label>
        <select id="tDept" data-change="refreshRespOptions">
          ${deptOptions.map(d=>`<option value="${d.id}">${htmlesc(d.name)}</option>`).join("")}
        </select>
      </div>

      <div class="field">
        <label>Відповідальний</label>
        <select id="tResp"></select>
      </div>
    ` : `
      <div class="field">
        <label>Відповідальний</label>
        <input value="Керівник (ви)" disabled />
      </div>
    `}

    <div class="row2">
      <div class="field">
        <label>Пріоритет</label>
        <select id="tPr">
          <option value="звичайний">Звичайний</option>
          <option value="високий">Високий</option>
          <option value="терміново">Терміново</option>
          <option value="низький">Низький</option>
        </select>
      </div>

      <div class="field">
        <label>Дедлайн</label>
        <div class="row" style="display:flex;gap:8px;">
          <input id="tDue" type="date" value="${addDays(today, 3)}" />
          <input id="tDueTime" type="time" value="" />
        </div>
      </div>
    </div>

    <div class="field">
      <label><input id="noDue" type="checkbox" data-change="toggleNoDue" /> Без дедлайну</label>
    </div>

    <div id="ctrlBlock">
      <div class="field">
        <label>Контрольна дата</label>
        <input id="tCtrl" type="date" value="${addDays(today, 1)}" />
      </div>
      <div class="field">
        <label><input id="tCtrlAlways" type="checkbox" data-change="toggleCtrlAlways" /> Постійний контроль (без дати)</label>
      </div>
      <div class="hint">Контроль використовується тільки якщо немає дедлайну.</div>
    </div>

    <div class="actions" style="margin-top:14px;">
      <button class="btn primary" data-action="createTaskNow" data-arg1="${kind}">Створити</button>
      <button class="btn ghost" data-action="hideSheet">Скасувати</button>
    </div>
  `);

  toggleNoDue();
  if(!isPersonal) refreshRespOptions();
}

function createTaskNow(kind){
  const u = currentSessionUser();
  const title = document.getElementById("tTitle").value.trim();
  if(!title){
    showSheet("Помилка", `<div class="hint">Вкажи назву задачі.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  const desc = document.getElementById("tDesc").value.trim();
  const pr = document.getElementById("tPr").value;
  const noDue = document.getElementById("noDue").checked;
  const ctrlAlways = noDue ? !!document.getElementById("tCtrlAlways")?.checked : false;
  const dueDateVal = document.getElementById("tDue").value || null;
  const dueTimeVal = document.getElementById("tDueTime")?.value || "";
  const due = noDue ? null : joinDateTime(dueDateVal, dueTimeVal);
  const ctrl = (noDue && !ctrlAlways) ? (document.getElementById("tCtrl").value || null) : null;

  if(!noDue && !dueDateVal){
    showSheet("Помилка", `<div class="hint">Вкажи дедлайн або вибери “Без дедлайну”.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const today = kyivDateStr();

  const type = kind;
  const idPrefix = (kind==="managerial") ? "T" : (kind==="internal" ? "I" : "P");
  const id = genTaskCode(idPrefix);

  let departmentId = null;
  let responsibleUserId = "u_boss";

  if(kind==="personal"){
    departmentId = null;
    responsibleUserId = "u_boss";
  } else {
    departmentId = document.getElementById("tDept").value;
    responsibleUserId = document.getElementById("tResp").value;
  }

  const status = "в_процесі";

  createTask({
    id,
    type,
    title,
    description: desc,
    departmentId,
    responsibleUserId,
    priority: pr,
    status,
    startDate: today,
    dueDate: due,
    nextControlDate: ctrl,
    controlAlways: ctrlAlways,
    createdBy: u.id,
    createdAt: nowIsoKyiv(),
    updatedAt: nowIsoKyiv()
  }, u.id);

  hideSheet();
  UI.tab = ROUTES.TASKS;
  UI.taskFilter = "активні";
  render();
}

function openCreateAnnouncement(){
  const u = currentSessionUser();
  if(!u || u.role!=="boss"){
    showSheet("Немає прав", `<div class="hint">Оголошення може створювати лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  showSheet("Нове оголошення", `
    <div class="field">
      <label>Аудиторія</label>
      <select id="aAudience">
        <option value="staff">Особовий склад</option>
        <option value="meeting">Нарада (керівництво)</option>
      </select>
    </div>
    <div class="field">
      <label>Заголовок</label>
      <input id="aTitle" />
    </div>
    <div class="actions" style="margin-top:14px;">
      <button class="btn primary" data-action="createAnnouncementNow">Зберегти</button>
      <button class="btn ghost" data-action="hideSheet">Скасувати</button>
    </div>
  `);
}

function createAnnouncementNow(){
  const u = currentSessionUser();
  if(!u || u.role!=="boss"){
    showSheet("Немає прав", `<div class="hint">Оголошення може створювати лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  const audience = document.getElementById("aAudience")?.value || "staff";
  const title = (document.getElementById("aTitle")?.value || "").trim();
  if(!title){
    showSheet("Помилка", `<div class="hint">Вкажи заголовок оголошення.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const today = kyivDateStr();
  const id = genTaskCode("A");
  createTask({
    id,
    type: "personal",
    title,
    description: "",
    departmentId: null,
    responsibleUserId: u.id,
    priority: null,
    status: "в_процесі",
    startDate: today,
    dueDate: null,
    nextControlDate: null,
    controlAlways: false,
    createdBy: u.id,
    createdAt: nowIsoKyiv(),
    updatedAt: nowIsoKyiv(),
    category: "announcement",
    audience
  }, u.id);

  hideSheet();
  UI.tab = ROUTES.TASKS;
  if(u.role==="boss") UI.taskDeptFilter = "personal";
  UI.taskPersonalFilter = "announcements";
  UI.taskFilter = "активні";
  render();
}

/* ===========================
   MISSING LIST
=========================== */
function groupBy(arr, fn){
  return arr.reduce((acc,x)=>{
    const k = fn(x) ?? "unknown";
    acc[k] = acc[k] || [];
    acc[k].push(x);
    return acc;
  },{});
}
function openMissing(){
  const u = currentSessionUser();
  const today = kyivDateStr();
  const weekend = isWeekend(kyivNow());

  const executors = (u.role==="boss")
    ? STATE.users.filter(x=>x.active && x.role==="executor")
    : STATE.users.filter(x=>x.active && x.role==="executor" && x.departmentId===u.departmentId);

  const reportsToday = STATE.dailyReports.filter(r=>r.reportDate===today);
  const missing = weekend ? [] : executors.filter(x=>!reportsToday.some(r=>r.userId===x.id));

  const grouped = groupBy(missing, x=>x.departmentId);
  const html = `
    <div class="hint">${weekend ? "Сьогодні вихідний — контроль звітів не обов’язковий." : "Список виконавців без звіту."}</div>
    <div class="sep"></div>
    ${
      Object.keys(grouped).length
      ? Object.entries(grouped).map(([deptId, users])=>{
          const dept = getDeptById(deptId);
          return `
            <div class="item" style="cursor:default;">
              <div class="row"><div class="name">${htmlesc(dept?.name ?? "")}</div><span class="badge b-danger mono">${users.length}</span></div>
              <div class="hint" style="margin-top:10px;">${users.map(x=>`• ${htmlesc(x.name)}`).join("<br/>")}</div>
            </div>
          `;
        }).join("")
      : `<div class="hint">Немає “не здали”.</div>`
    }
    <div class="sep"></div>
    <button class="btn primary" data-action="hideSheet">Закрити</button>
  `;
  showSheet("Не здали", html);
}

/* ===========================
   DELEGATIONS (boss only)
=========================== */
function createDelegation({departmentId, actingHeadUserId, startDate, endDate, untilCancel}){
  const primary = STATE.users.find(u=>u.role==="dept_head" && u.departmentId===departmentId);
  if(!primary) throw new Error("Немає начальника відділу в довіднику.");

  STATE.delegations = STATE.delegations.map(d=>{
    if(d.departmentId!==departmentId) return d;
    if(d.status==="скасовано" || d.status==="завершено") return d;
    return {...d, status:"завершено", endedAt: nowIsoKyiv()};
  });

  STATE.delegations.push({
    id: uid("del"),
    departmentId,
    primaryHeadUserId: primary.id,
    actingHeadUserId,
    startDate,
    endDate: untilCancel ? null : endDate,
    untilCancel: !!untilCancel,
    status: "заплановано",
    createdAt: nowIsoKyiv(),
    createdBy: "u_boss",
  });

  recomputeDelegationStatuses();
  saveState(STATE);
}
function cancelDelegation(delegationId){
  const idx = STATE.delegations.findIndex(d=>d.id===delegationId);
  if(idx<0) return;
  STATE.delegations[idx] = {...STATE.delegations[idx], status:"скасовано", cancelledAt: nowIsoKyiv()};
  saveState(STATE);
}

function openDelegations(){
  const u = currentSessionUser();
  if(u.role!=="boss"){
    showSheet("Немає прав", `<div class="hint">Заміщення призначає тільки керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }
  recomputeDelegationStatuses();

  const active = STATE.delegations.filter(d=>d.status==="активне");
  const scheduled = STATE.delegations.filter(d=>d.status==="заплановано");

  const renderDel = (d)=>{
    const dept = getDeptById(d.departmentId);
    const prim = getUserById(d.primaryHeadUserId);
    const act = getUserById(d.actingHeadUserId);
    const period = d.untilCancel ? `з ${fmtDate(d.startDate)} • до скасування` : `з ${fmtDate(d.startDate)} по ${fmtDate(d.endDate)}`;
    const stCls = d.status==="активне" ? "b-blue" : "b-warn";
    const stLbl = d.status==="активне" ? "АКТИВНЕ" : "ЗАПЛАНОВАНО";

    return `
      <div class="item" style="cursor:default;">
        <div class="row">
          <div>
            <div class="name">${deptBadgeHtml(dept)}</div>
            <div class="sub">
              <span class="badge ${stCls}">${stLbl}</span>
              <span class="pill">Нач.: ${htmlesc(prim?.name ?? "")}</span>
              <span class="pill">В.о.: ${htmlesc(act?.name ?? "")}</span>
            </div>
            <div class="hint" style="margin-top:10px;">Період: <span class="mono">${htmlesc(period)}</span></div>
          </div>
        </div>
        <div class="actions">
          <button class="btn danger" data-action="cancelDelegationUi" data-arg1="${d.id}">Скасувати</button>
        </div>
      </div>
    `;
  };

  showSheet("Заміщення (в.о.)", `
    <div class="hint">Тут ти призначаєш в.о. начальника відділу. Відділ має лише одного активного керівника на дату.</div>
    <div class="sep"></div>

    <div class="item" style="cursor:default;">
      <div class="row"><div class="name">Активні</div><span class="badge b-blue mono">${active.length}</span></div>
    </div>
    ${active.map(renderDel).join("") || `<div class="hint">Немає активних.</div>`}

    <div class="sep"></div>

    <div class="item" style="cursor:default;">
      <div class="row"><div class="name">Заплановані</div><span class="badge b-warn mono">${scheduled.length}</span></div>
    </div>
    ${scheduled.map(renderDel).join("") || `<div class="hint">Немає запланованих.</div>`}

    <div class="sep"></div>
    <button class="btn primary" data-action="openDelegationCreate">➕ Додати заміщення</button>
    <button class="btn ghost" data-action="hideSheet">Закрити</button>
  `);
}

function cancelDelegationUi(id){
  const d = STATE.delegations.find(x=>x.id===id);
  if(!d){
    cancelDelegation(id);
    hideSheet();
    openDelegations();
    return;
  }
  const dept = getDeptById(d.departmentId)?.name || "Відділ";
  const acting = getUserById(d.actingUserId)?.name || "—";
  showSheet("Скасувати заміщення", `
    <div class="hint">Скасувати заміщення у <b>${htmlesc(dept)}</b> (в.о.: <b>${htmlesc(acting)}</b>)?</div>
    <div class="actions" style="margin-top:14px;">
      <button class="btn danger" data-action="confirmCancelDelegation" data-arg1="${d.id}">Скасувати</button>
      <button class="btn ghost" data-action="hideSheet">Назад</button>
    </div>
  `);
}

function openControlByDept(){
  const u = currentSessionUser();
  if(!u || u.role!=="boss"){
    showSheet("Немає прав", `<div class="hint">Цей екран доступний тільки керівнику.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
    return;
  }

  const tasks = getVisibleTasksForUser(u).filter(t=>!!t.departmentId);
  const notBlocked = (t)=>!["блокер","очікування"].includes(t.status);
  const tasksDate = tasks.filter(t=>t.nextControlDate && !t.controlAlways && notBlocked(t));
  const tasksAlways = tasks.filter(t=>t.controlAlways && !t.nextControlDate && notBlocked(t));
  const tasksDeadline = tasks.filter(t=>t.dueDate && t.status!=="закрито" && t.status!=="скасовано");

  const byDept = STATE.departments.map(d=>{
    const deadline = tasksDeadline
      .filter(t=>t.departmentId===d.id)
      .sort((a,b)=>dueSortKey(a.dueDate).localeCompare(dueSortKey(b.dueDate)));
    const ctrlDate = tasksDate
      .filter(t=>t.departmentId===d.id)
      .sort((a,b)=>(a.nextControlDate || "9999-99-99").localeCompare(b.nextControlDate || "9999-99-99"));
    const ctrlAlways = tasksAlways.filter(t=>t.departmentId===d.id);
    const total = deadline.length + ctrlDate.length + ctrlAlways.length;
    return {dept:d, deadline, ctrlDate, ctrlAlways, total};
  }).filter(x=>x.total>0);

  const renderRows = (list, suffixFn)=> list.map(t=>{
    const suffix = suffixFn ? suffixFn(t) : "";
    const rowCls = isOverdue(t) ? "control-task overdue-line" : "control-task";
    return `<span class="${rowCls}">• <b>${htmlesc(t.title)}</b>${htmlesc(suffix)}</span>`;
  }).join("<br/>");

  const renderSection = (title, icon, list, suffixFn)=>`
    <div class="control-dept-section">
      <div class="control-dept-section-h">${icon} ${title} <span class="mono">${list.length}</span></div>
      <div class="control-dept-section-b">
        ${list.length ? renderRows(list, suffixFn) : `<span class="hint">Немає</span>`}
      </div>
    </div>
  `;

  const html = `
    <div class="control-dept-grid">
      ${byDept.map(x=>`
        <div class="control-dept-card">
          <div class="control-dept-head">
            <div class="control-dept-name">${deptBadgeHtml(x.dept)} <span class="mono">${x.total}</span></div>
            <div class="control-dept-counts">
              <span class="pill mono">⏱ ${x.deadline.length}</span>
              <span class="pill mono">🗓 ${x.ctrlDate.length}</span>
              <span class="pill mono">🎯 ${x.ctrlAlways.length}</span>
            </div>
          </div>
          <div class="control-dept-body">
            ${renderSection("Дедлайн", "⏱", x.deadline, (t)=> t.dueDate ? ` — ${dueTitle(t.dueDate)}` : "")}
            ${renderSection("Контроль з датою", "🗓", x.ctrlDate, (t)=> t.nextControlDate ? ` — ${fmtDate(t.nextControlDate)}` : "")}
            ${renderSection("Постійний контроль", "🎯", x.ctrlAlways)}
          </div>
        </div>
      `).join("")}
      ${byDept.length ? "" : `<div class="hint">Немає задач.</div>`}
    </div>
  `;

  showSheet("Контроль по відділах", `
    ${html}
    <div class="sep"></div>
    <button class="btn primary" data-action="hideSheet">Закрити</button>
  `);
}
function confirmCancelDelegation(id){
  cancelDelegation(id);
  hideSheet();
  openDelegations();
}

function refreshDelPeople(){
  const dept = document.getElementById("dDept");
  const actingSelect = document.getElementById("dAct");
  if(!dept || !actingSelect) return;

  const deptId = dept.value;
  const primary = STATE.users.find(u=>u.role==="dept_head" && u.departmentId===deptId);
  const candidates = STATE.users.filter(u=>u.active && u.departmentId===deptId && u.id !== primary?.id);
  actingSelect.innerHTML = candidates.map(c=>`<option value="${c.id}">${htmlesc(c.name)} (${c.role})</option>`).join("");
}

function openDelegationCreate(){
  const deptOptions = STATE.departments;
  const today = kyivDateStr();

  showSheet("Нове заміщення", `
    <div class="hint">Призначення <b>в.о.</b> (лише керівник).</div>

    <div class="field">
      <label>Відділ</label>
      <select id="dDept" data-change="refreshDelPeople">
        ${deptOptions.map(d=>`<option value="${d.id}">${htmlesc(d.name)}</option>`).join("")}
      </select>
    </div>

    <div class="field">
      <label>В.о. начальника</label>
      <select id="dAct"></select>
    </div>

    <div class="row2">
      <div class="field">
        <label>Початок</label>
        <input id="dStart" type="date" value="${today}" />
      </div>
      <div class="field">
        <label>Кінець</label>
        <input id="dEnd" type="date" value="${addDays(today, 7)}" />
      </div>
    </div>

    <div class="field">
      <label><input id="dUntil" type="checkbox" /> До скасування</label>
    </div>

    <div class="actions" style="margin-top:14px;">
      <button class="btn primary" data-action="createDelegationNow">Зберегти</button>
      <button class="btn ghost" data-action="openDelegations">Назад</button>
    </div>
  `);
  refreshDelPeople();
}

function createDelegationNow(){
  try{
    const deptId = document.getElementById("dDept").value;
    const act = document.getElementById("dAct").value;
    const start = document.getElementById("dStart").value;
    const end = document.getElementById("dEnd").value;
    const until = document.getElementById("dUntil").checked;
    if(!deptId || !act || !start || (!until && !end)){
      showSheet("Помилка", `<div class="hint">Заповни обов’язкові поля.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
      return;
    }
    createDelegation({departmentId:deptId, actingHeadUserId:act, startDate:start, endDate:end, untilCancel:until});
    hideSheet();
    openDelegations();
    render();
  } catch(err){
    showSheet("Помилка", `<div class="hint">${htmlesc(err.message || "Не вдалося створити заміщення.")}</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);
  }
}

/* ===========================
   PROFILE VIEW
=========================== */
function viewProfile(){
  if(!ensureLoggedIn()) return viewLogin();
  recomputeDelegationStatuses();

  const u = currentSessionUser();
  UI.route = ROUTES.PROFILE;

  const dept = u.departmentId ? getDeptById(u.departmentId) : null;
  const {isDeptHeadLike} = asDeptRole(u);
  const roleText = (u.role==="boss") ? "Керівник" : (isDeptHeadLike ? "Начальник відділу / в.о." : "Виконавець");
  const delBanner = actingBannerForUser(u);

  const body = `
    <div class="card">
      <div class="card-h">
        <div class="t">Профіль</div>
        <span class="badge b-blue">${htmlesc(roleText)}</span>
      </div>
      <div class="card-b">
        <div class="item" style="cursor:default;">
          <div class="row">
            <div>
              <div class="name">${htmlesc(u.name)}</div>
              <div class="sub">
                <span class="pill mono">${htmlesc(u.login)}</span>
                ${dept ? `<span class="pill">${htmlesc(dept.name)}</span>` : `<span class="pill">Всі відділи</span>`}
              </div>
            </div>
          </div>
          ${delBanner ? `<div class="hint" style="margin-top:10px;">${htmlesc(delBanner)}</div>` : ``}
        </div>

        <div class="actions">
          ${u.role==="boss" ? `<button class="btn primary" data-action="openDelegations">🧩 Заміщення (в.о.)</button>` : ``}
          <button class="btn ghost" data-action="openAbout">ℹ️ Про прототип</button>
          <button class="btn danger" data-action="logout">🚪 Вийти</button>
        </div>

        <div class="hint">
          Для імітації начальника: вийди → зайди як <span class="mono">head2</span> або <span class="mono">head5</span>.
        </div>
      </div>
    </div>
  `;

  const tabs = (u.role==="boss")
    ? [
      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},
      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},
      {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},
    ]
    : [
      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},
      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},
    ];

  const subtitle = (u.role==="boss") ? "Керівник" : (dept?.name ?? "Відділ");
  appShell({title:"Профіль", subtitle, bodyHtml: body, showFab:false, fabAction:null, tabs});
}

function openAbout(){
  showSheet("Про прототип", `
    <div class="hint">
      Це <b>mobile-first</b> прототип без сервера.<br/>
      Дані зберігаються в <b>localStorage</b> (тільки для тестування).<br/><br/>
      Реалізовано:
      <ul>
        <li>Логін/пароль</li>
        <li>Ролі: керівник / начальник відділу / виконавець</li>
        <li>Заміщення (в.о.) — призначає лише керівник</li>
        <li>Задачі: управлінські + внутрішні + <b>мої (керівника)</b></li>
        <li>Задачі <b>без дедлайну</b> + контрольна дата</li>
        <li>Щоденні звіти (ПІЗНО після 17:30)</li>
        <li>Підсумок відділу (3–5 речень)</li>
        <li>Люди/штат у начальника: хто здав/не здав</li>
        <li>Керівник у “Звітах”: “Хто не здав” по відділах + “👥 Люди”</li>
      </ul>
    </div>
    <div class="sep"></div>
    <button class="btn primary" data-action="hideSheet">OK</button>
  `);
}

/* ===========================
   ANALYTICS VIEW (demo)
=========================== */
function viewAnalytics(){
  const u = currentSessionUser();
  if(!u || u.role!=="boss"){ UI.tab = ROUTES.CONTROL; return viewControl(); }
  UI.tab = ROUTES.ANALYTICS;

  const today = kyivDateStr();
  const days = Array.from({length:7}, (_,i)=>addDays(today, -(6-i)));
  const closeDateForTask = (task)=>{
    const updates = STATE.taskUpdates
      .filter(u=>u.taskId===task.id && u.status==="закрито")
      .sort((a,b)=>b.at.localeCompare(a.at));
    if(updates[0]) return toDateOnly(updates[0].at);
    if(task.status==="закрито") return toDateOnly(task.updatedAt);
    return null;
  };
  const weekClosed = days.map(d=>{
    const count = STATE.tasks.filter(t=>closeDateForTask(t)===d).length;
    return {date:d, count};
  });
  const maxClosed = Math.max(1, ...weekClosed.map(x=>x.count));

  const closedDurations = STATE.tasks
    .map(t=>{
      const closeDate = closeDateForTask(t);
      const startDate = toDateOnly(t.createdAt) || t.startDate;
      if(!closeDate || !startDate) return null;
      const daysToClose = dateDiffDays(startDate, closeDate);
      if(daysToClose < 0) return null;
      return {task:t, daysToClose};
    })
    .filter(Boolean);
  const avgClose = closedDurations.length
    ? (closedDurations.reduce((s,x)=>s+x.daysToClose, 0) / closedDurations.length).toFixed(1)
    : "—";

  const topProblems = STATE.tasks
    .map(t=>{
      const blockerUpdates = STATE.taskUpdates.filter(u=>u.taskId===t.id && (u.status==="блокер" || u.status==="очікування"));
      return {task:t, count:blockerUpdates.length, last:blockerUpdates.sort((a,b)=>b.at.localeCompare(a.at))[0]};
    })
    .filter(x=>x.count>0)
    .sort((a,b)=>b.count-a.count)
    .slice(0,5);

  const deptLoad = STATE.departments.map(d=>{
    const deptTasks = STATE.tasks.filter(t=>t.departmentId===d.id);
    const active = deptTasks.filter(t=>t.status!=="закрито" && t.status!=="скасовано").length;
    const blockers = deptTasks.filter(t=>t.status==="блокер" || t.status==="очікування").length;
    const overdue = deptTasks.filter(t=>isOverdue(t)).length;
    return {dept:d, active, blockers, overdue};
  });
  const maxActive = Math.max(1, ...deptLoad.map(x=>x.active));

  const activeDeptTasks = STATE.tasks.filter(t=>t.departmentId && t.status!=="закрито" && t.status!=="скасовано");
  const recentClosed = STATE.tasks.filter(t=>t.departmentId && t.status==="закрито" && closeDateForTask(t) && closeDateForTask(t) >= days[0] && closeDateForTask(t) <= days[days.length-1]);
  const priorityKeys = ["терміново","високий","звичайний","низький"];
  const priorityLabels = {
    "терміново":"Терміново",
    "високий":"Високий",
    "звичайний":"Звичайний",
    "низький":"Низький"
  };
  const priorityCounts = priorityKeys.map(k=>({
    key:k,
    label: priorityLabels[k] || k,
    count: activeDeptTasks.filter(t=>t.priority===k).length
  }));
  const priorityOther = activeDeptTasks.filter(t=>!priorityKeys.includes(t.priority)).length;
  if(priorityOther>0){
    priorityCounts.push({key:"other", label:"Без пріоритету", count: priorityOther});
  }
  const maxPriority = Math.max(1, ...priorityCounts.map(x=>x.count));
  const priorityClosed = priorityKeys.map(k=>({
    key:k,
    label: priorityLabels[k] || k,
    count: recentClosed.filter(t=>t.priority===k).length
  }));
  const priorityClosedOther = recentClosed.filter(t=>!priorityKeys.includes(t.priority)).length;
  if(priorityClosedOther>0){
    priorityClosed.push({key:"other", label:"Без пріоритету", count: priorityClosedOther});
  }
  const maxPriorityClosed = Math.max(1, ...priorityClosed.map(x=>x.count));

  const priorityBreakdown = (list)=>{
    const rows = priorityKeys.map(k=>({
      key:k,
      label: priorityLabels[k] || k,
      count: list.filter(t=>t.priority===k).length
    }));
    const other = list.filter(t=>!priorityKeys.includes(t.priority)).length;
    if(other>0){
      rows.push({key:"other", label:"Без пріоритету", count: other});
    }
    const max = Math.max(1, ...rows.map(x=>x.count));
    return {rows, max, total: list.length};
  };

  const activeDeadline = activeDeptTasks.filter(t=>!!t.dueDate);
  const activeCtrlDate = activeDeptTasks.filter(t=>!t.dueDate && !!t.nextControlDate && !t.controlAlways);
  const activeCtrlAlways = activeDeptTasks.filter(t=>!t.dueDate && !!t.controlAlways);
  const closedDeadline = recentClosed.filter(t=>!!t.dueDate);
  const closedCtrlDate = recentClosed.filter(t=>!t.dueDate && !!t.nextControlDate && !t.controlAlways);
  const closedCtrlAlways = recentClosed.filter(t=>!t.dueDate && !!t.controlAlways);

  const priActiveDeadline = priorityBreakdown(activeDeadline);
  const priActiveCtrlDate = priorityBreakdown(activeCtrlDate);
  const priActiveCtrlAlways = priorityBreakdown(activeCtrlAlways);
  const priClosedDeadline = priorityBreakdown(closedDeadline);
  const priClosedCtrlDate = priorityBreakdown(closedCtrlDate);
  const priClosedCtrlAlways = priorityBreakdown(closedCtrlAlways);

  const body = `
    <div class="card">
      <div class="card-h">
        <div class="t">Аналітика</div>
        <span class="badge b-blue">Останні 7 днів</span>
      </div>
      <div class="card-b analytics-grid">
        <div class="item analytics-block" style="cursor:default;">
          <div class="row"><div class="name">Графік закриття задач</div><span class="badge b-ok mono">${weekClosed.reduce((s,x)=>s+x.count,0)}</span></div>
          <div class="hint">Скільки задач закрито по днях.</div>
          <div class="analytics-bars">
            ${weekClosed.map(x=>`
              <div class="analytics-bar-row">
                <div class="analytics-label mono">${fmtDate(x.date).slice(0,5)}</div>
                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/maxClosed)*100)}%"></div></div>
                <div class="analytics-value mono">${x.count}</div>
              </div>
            `).join("")}
          </div>
        </div>

        <div class="item analytics-block" style="cursor:default;">
          <div class="row"><div class="name">Середній час закриття</div><span class="badge b-violet mono">${avgClose}</span></div>
          <div class="hint">Середня тривалість від створення до закриття (у днях).</div>
        </div>

        <div class="item analytics-block" style="cursor:default;">
          <div class="row"><div class="name">Топ проблем</div><span class="badge b-warn mono">${topProblems.length}</span></div>
          <div class="hint">
            ${
              topProblems.length
                ? topProblems.map(x=>{
                    const dept = x.task.departmentId ? getDeptById(x.task.departmentId)?.name : "Особисто";
                    const note = x.last?.note ? ` — ${htmlesc(x.last.note).slice(0,80)}` : "";
                    return `• <b>${htmlesc(x.task.title)}</b> (${htmlesc(dept || "")}) • ${x.count}${note}`;
                  }).join("<br/>")
                : "Немає активних блокерів."
            }
          </div>
        </div>

        <div class="item analytics-block" style="cursor:default;">
          <div class="row"><div class="name">Навантаження по відділах</div><span class="badge b-blue mono">${deptLoad.reduce((s,x)=>s+x.active,0)}</span></div>
          <div class="analytics-bars">
            ${deptLoad.map(x=>`
              <div class="analytics-bar-row">
                <div class="analytics-label">${htmlesc(x.dept.name)}</div>
                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.active/maxActive)*100)}%"></div></div>
                <div class="analytics-value mono">${x.active}</div>
              </div>
              <div class="hint" style="margin-top:2px;">⛔ ${x.blockers} • 🟠 ${x.overdue}</div>
            `).join("")}
          </div>
        </div>

        <div class="item analytics-block" style="cursor:default;">
          <div class="row"><div class="name">Пріоритети задач</div><span class="badge b-blue mono">${activeDeptTasks.length}</span></div>
          <div class="hint">Активні задачі по пріоритетах.</div>
          <div class="analytics-bars">
            ${priorityCounts.map(x=>`
              <div class="analytics-bar-row">
                <div class="analytics-label">${htmlesc(x.label)}</div>
                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/maxPriority)*100)}%"></div></div>
                <div class="analytics-value mono">${x.count}</div>
              </div>
            `).join("")}
          </div>
        </div>

        <div class="item analytics-block" style="cursor:default;">
          <div class="row"><div class="name">Активні з дедлайном — пріоритети</div><span class="badge b-blue mono">${priActiveDeadline.total}</span></div>
          <div class="hint">Активні задачі з дедлайном.</div>
          <div class="analytics-bars">
            ${priActiveDeadline.rows.map(x=>`
              <div class="analytics-bar-row">
                <div class="analytics-label">${htmlesc(x.label)}</div>
                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/priActiveDeadline.max)*100)}%"></div></div>
                <div class="analytics-value mono">${x.count}</div>
              </div>
            `).join("")}
          </div>
        </div>

        <div class="item analytics-block" style="cursor:default;">
          <div class="row"><div class="name">Активні з датою контролю — пріоритети</div><span class="badge b-blue mono">${priActiveCtrlDate.total}</span></div>
          <div class="hint">Активні задачі з контрольними датами.</div>
          <div class="analytics-bars">
            ${priActiveCtrlDate.rows.map(x=>`
              <div class="analytics-bar-row">
                <div class="analytics-label">${htmlesc(x.label)}</div>
                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/priActiveCtrlDate.max)*100)}%"></div></div>
                <div class="analytics-value mono">${x.count}</div>
              </div>
            `).join("")}
          </div>
        </div>

        <div class="item analytics-block" style="cursor:default;">
          <div class="row"><div class="name">Активні на постійному контролі — пріоритети</div><span class="badge b-blue mono">${priActiveCtrlAlways.total}</span></div>
          <div class="hint">Активні задачі на постійному контролі.</div>
          <div class="analytics-bars">
            ${priActiveCtrlAlways.rows.map(x=>`
              <div class="analytics-bar-row">
                <div class="analytics-label">${htmlesc(x.label)}</div>
                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/priActiveCtrlAlways.max)*100)}%"></div></div>
                <div class="analytics-value mono">${x.count}</div>
              </div>
            `).join("")}
          </div>
        </div>

        <div class="item analytics-block" style="cursor:default;">
          <div class="row"><div class="name">Пріоритети закритих (7 днів)</div><span class="badge b-ok mono">${recentClosed.length}</span></div>
          <div class="hint">Закриті задачі за останні 7 днів.</div>
          <div class="analytics-bars">
            ${priorityClosed.map(x=>`
              <div class="analytics-bar-row">
                <div class="analytics-label">${htmlesc(x.label)}</div>
                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/maxPriorityClosed)*100)}%"></div></div>
                <div class="analytics-value mono">${x.count}</div>
              </div>
            `).join("")}
          </div>
        </div>

        <div class="item analytics-block" style="cursor:default;">
          <div class="row"><div class="name">Закриті з дедлайном — пріоритети (7 днів)</div><span class="badge b-ok mono">${priClosedDeadline.total}</span></div>
          <div class="hint">Закриті задачі з дедлайном за останні 7 днів.</div>
          <div class="analytics-bars">
            ${priClosedDeadline.rows.map(x=>`
              <div class="analytics-bar-row">
                <div class="analytics-label">${htmlesc(x.label)}</div>
                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/priClosedDeadline.max)*100)}%"></div></div>
                <div class="analytics-value mono">${x.count}</div>
              </div>
            `).join("")}
          </div>
        </div>

        <div class="item analytics-block" style="cursor:default;">
          <div class="row"><div class="name">Закриті з датою контролю — пріоритети (7 днів)</div><span class="badge b-ok mono">${priClosedCtrlDate.total}</span></div>
          <div class="hint">Закриті задачі з контрольними датами за останні 7 днів.</div>
          <div class="analytics-bars">
            ${priClosedCtrlDate.rows.map(x=>`
              <div class="analytics-bar-row">
                <div class="analytics-label">${htmlesc(x.label)}</div>
                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/priClosedCtrlDate.max)*100)}%"></div></div>
                <div class="analytics-value mono">${x.count}</div>
              </div>
            `).join("")}
          </div>
        </div>

        <div class="item analytics-block" style="cursor:default;">
          <div class="row"><div class="name">Закриті на постійному контролі — пріоритети (7 днів)</div><span class="badge b-ok mono">${priClosedCtrlAlways.total}</span></div>
          <div class="hint">Закриті задачі на постійному контролі за останні 7 днів.</div>
          <div class="analytics-bars">
            ${priClosedCtrlAlways.rows.map(x=>`
              <div class="analytics-bar-row">
                <div class="analytics-label">${htmlesc(x.label)}</div>
                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/priClosedCtrlAlways.max)*100)}%"></div></div>
                <div class="analytics-value mono">${x.count}</div>
              </div>
            `).join("")}
          </div>
        </div>
      </div>
    </div>
  `;

  const tabs = [
    {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},
    {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},
    {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},
  ];

  appShell({title:"Аналітика", subtitle:"Керівник", bodyHtml: body, showFab:false, fabAction:null, tabs});
}

/* ===========================
   TASK LIST SHORTCUT
=========================== */
function openTaskList(filterKey){
  UI.tab = ROUTES.TASKS;
  UI.taskFilter = filterKey;
  render();
}
function openHelp(){
  showSheet("Довідка користувача", `
    <div class="item" style="cursor:default;">
      <div class="name">Що це за програма</div>
      <div class="hint">Планувальник задач, щоденних звітів і контролю виконання для керівника, начальників відділів і виконавців.</div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">Основні екрани і логіка</div>
      <div class="hint">
        🧭 Контроль: що критично сьогодні (не здали звіт, блокери, задачі на підтвердження).<br/>
        📝 Звіти: щоденні звіти виконавців та підсумки відділів.<br/>
        📋 Задачі: постановка, виконання, фільтри по статусах і відділах.<br/>
        📈 Аналітика: динаміка закриття, топ проблем, середній час закриття, навантаження відділів.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">Роль: Керівник (boss) — приклад дня</div>
      <div class="hint">
        1) Відкрий “Контроль” і перевір плитку “Очікує підтвердження”.<br/>
        2) Перейди в “Задачі” → фільтр “Очікує підтвердження”.<br/>
        3) Відкрий картку задачі, перевір оновлення, натисни “Підтвердити” або “Повернути”.<br/>
        4) У “Аналітиці” оціни, де ростуть блокери і який відділ перевантажений.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">Роль: Начальник відділу (head) — приклад дня</div>
      <div class="hint">
        1) Отримай управлінську задачу від керівника.<br/>
        2) Розбий роботу на внутрішні задачі для виконавців.<br/>
        3) В кінці дня перевір “Люди/штат” (хто здав/не здав).<br/>
        4) Подай підсумок відділу (3–5 речень) у вкладці “Контроль”.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">Роль: Виконавець — приклад дня</div>
      <div class="hint">
        1) Відкрий свої задачі, онови статус (в процесі/блокер).<br/>
        2) Якщо є проблема — зафіксуй блокер у задачі та у щоденному звіті.<br/>
        3) Подай щоденний звіт до 17:30, щоб не було позначки “ПІЗНО”.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">Аналітика: як читати</div>
      <div class="hint">
        Графік закриття: якщо падає 2–3 дні поспіль — відділ “застряг”.<br/>
        Топ проблем: задачі, що найчастіше переходили в блокер/очікування.<br/>
        Середній час закриття: орієнтир швидкості виконання.<br/>
        Навантаження відділів: порівняння активних задач + блокерів/прострочених.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">Експорт у Excel</div>
      <div class="hint">
        У “Задачах” натисни ⬇️ Excel, обери період і завантаж файл.<br/>
        Книга містить вкладку “Загальні” і окремі вкладки по відділах.
      </div>
    </div>

    <div class="sep"></div>
    <button class="btn primary" data-action="hideSheet">Зрозуміло</button>
  `);
}

const ACTIONS = {
  applyControlDate,
  cancelDelegationUi,
  confirmCancelDelegation,
  confirmTaskClose,
  createDelegationNow,
  createAnnouncementNow,
  createTaskNow,
  goProfile,
  hideSheet,
  logout,
  openAbout,
  openHelp,
  openCreateTask,
  openCreateAnnouncement,
  openDelegationCreate,
  openDelegations,
  openDeptPeople,
  openDeptPeopleBoss,
  openDeptSummary,
  openDeptSummaryForm,
  openControlByDept,
  openMissing,
  openReport,
  openReportForm,
  openTasksExportDialog,
  openTask,
  openEditTask,
  openTaskList,
  toggleTaskScope,
  clearTaskSearch,
  setControlDate,
  applyReportTemplate,
  autoFillReport,
  saveTaskEdits,
  saveAnnouncementEdits,
  setTaskDeptFilter,
  setTaskPersonalFilter,
  setReportFilter,
  setTab,
  setTaskFilter,
  setTaskStatus,
  submitDeptSummaryNow,
  submitReportNow,
  submitStatusReason,
  exportTasksExcelNow,
  toggleTheme,
};
const CHANGE_ACTIONS = {
  refreshDelPeople,
  refreshRespOptions,
  setTaskSearchFromInput,
  setReportsControlDateFromInput,
  toggleNoDue,
  toggleCtrlAlways,
};

function runMappedAction(name, arg1, arg2){
  const action = ACTIONS[name];
  if(typeof action !== "function") return;
  if(typeof arg2 !== "undefined") return action(arg1, arg2);
  if(typeof arg1 !== "undefined") return action(arg1);
  return action();
}
function runMappedChange(name){
  const action = CHANGE_ACTIONS[name];
  if(typeof action === "function") action();
}

/* ===========================
   AUTO SYNC
=========================== */
function stateStamp(st){
  return (st && st.sync && st.sync.updatedAt) ? st.sync.updatedAt : "";
}
function queueSync(){
  if(!SYNC_URL) return;
  if(!_syncReady){
    _syncPending = true;
    return;
  }
  _syncPending = false;
  if(_syncTimer) clearTimeout(_syncTimer);
  _syncTimer = setTimeout(pushSync, SYNC_DEBOUNCE_MS);
}
async function pushSync(){
  if(!SYNC_URL || _syncInFlight || !_syncReady) return;
  _syncInFlight = true;
  try{
    ensureSyncMeta(STATE);
    const payload = { state: stateForSync(STATE) };
    const res = await fetch(SYNC_URL, {
      method: "PUT",
      headers: {"Content-Type":"application/json"},
      credentials: "include",
      body: JSON.stringify(payload),
    });
    if(res.ok){
      _lastPushAt = nowIsoKyiv();
    }
  } catch{}
  _syncInFlight = false;
}
async function pullSync(){
  if(!SYNC_URL || _syncInFlight) return;
  _syncInFlight = true;
  const wasInitDone = _syncInitDone;
  try{
    const res = await fetch(SYNC_URL, {credentials: "include"});
    if(!res.ok){
      _syncInFlight = false;
      _syncInitDone = true;
      if(!wasInitDone) render();
      return;
    }
    const data = await res.json();
    if(!data || !data.state){
      _syncInFlight = false;
      _syncInitDone = true;
      if(!wasInitDone) render();
      return;
    }
    const localUserId = STATE?.session?.userId || null;
    const remote = migrateState(data.state) || data.state;
    remote.session = {userId: localUserId};
    const isFirstSync = !_syncReady;
    _syncReady = true;
    _syncInitDone = true;
    if(isFirstSync){
      STATE = remote;
      saveState(STATE, {skipSyncStamp:true});
      _syncPending = false;
      render();
      _lastPullAt = nowIsoKyiv();
      _syncInFlight = false;
      return;
    }
    const localStamp = stateStamp(STATE);
    const remoteStamp = stateStamp(remote);
    if(remoteStamp && (!localStamp || remoteStamp > localStamp)){
      STATE = remote;
      saveState(STATE, {skipSyncStamp:true});
      _syncPending = false;
      render();
    }
    _lastPullAt = nowIsoKyiv();
    if(_syncPending) queueSync();
  } catch{}
  _syncInFlight = false;
  if(!_syncInitDone){
    _syncInitDone = true;
    if(!wasInitDone) render();
  }
}
function initAutoSync(){
  if(!SYNC_URL) return;
  pullSync();
  setInterval(pullSync, SYNC_POLL_MS);
  document.addEventListener("visibilitychange", ()=>{
    if(!document.hidden) pullSync();
  });
  window.addEventListener("online", pullSync);
}

function refreshOverdueClasses(){
  const items = document.querySelectorAll(".task-item[data-task-id]");
  if(!items.length) return;
  items.forEach(el=>{
    const id = el.dataset.taskId;
    if(!id) return;
    const t = STATE.tasks.find(x=>x.id===id);
    if(!t) return;
    el.classList.toggle("is-overdue", isOverdue(t));
  });
}
function initOverdueTicker(){
  refreshOverdueClasses();
  if(_overdueTimer) clearInterval(_overdueTimer);
  _overdueTimer = setInterval(refreshOverdueClasses, 30000);
  document.addEventListener("visibilitychange", ()=>{
    if(!document.hidden) refreshOverdueClasses();
  });
}

document.addEventListener("click", (e)=>{
  const el = e.target.closest("[data-action]");
  if(!el) return;
  e.preventDefault();

  if(el.dataset.action === "hideThen"){
    hideSheet();
    return runMappedAction(el.dataset.next, el.dataset.arg1, el.dataset.arg2);
  }
  runMappedAction(el.dataset.action, el.dataset.arg1, el.dataset.arg2);
});
document.addEventListener("change", (e)=>{
  const el = e.target.closest("[data-change]");
  if(!el) return;
  runMappedChange(el.dataset.change);
});

/* ===========================
   RENDER
=========================== */
function render(){
  const user = currentSessionUser();
  if(!user){
    UI.route = ROUTES.LOGIN;
    return viewLogin();
  }

  if(UI.route === ROUTES.PROFILE) return viewProfile();

  if(UI.tab === ROUTES.CONTROL) return viewControl();
  if(UI.tab === ROUTES.REPORTS) return viewReports();
  if(UI.tab === ROUTES.TASKS) return viewTasks();
  if(UI.tab === ROUTES.ANALYTICS) return viewAnalytics();

  return viewControl();
}

/* ===========================
   START
=========================== */
applyTheme(UI.theme);
render();
initAutoSync();
initOverdueTicker();

