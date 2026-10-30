const protobuf = require("protobufjs");
const { faker } = require("@faker-js/faker");
const { Kafka, Partitioners } = require("kafkajs");
const fs = require("fs");
const path = require("path");

const planPath = path.resolve(process.argv[2] || "plans/plan.json");
const planDir = path.dirname(planPath);
const plan = JSON.parse(fs.readFileSync(planPath, "utf8"));

// ---- merge defaults + events ----
const clean = (o) => Object.fromEntries(Object.entries(o).filter(([, v]) => v !== null));
const D = plan.defaults || {};
const events = plan.events.map((e) => {
  const m = { ...D, ...e, rules: clean({ ...D.rules, ...e.rules }), headers: clean({ ...D.headers, ...e.headers }) };
  if (m.pool) m.pools = { ...m.pools, [m.pool.name]: m.pool.add }; // old single-pool syntax
  delete m.pool;
  m.pools ??= {};
  return m;
});
events.forEach((e) => {
  if (!e.type || !e.proto || !e.topic) throw Error(`event needs type, proto, topic: ${JSON.stringify(e.type)}`);
});

// ---- weights or quotas ----
// weights: plan.count messages, event picked by weight
// quotas:  every event has an exact count, picked by how many it has left
const quotas = events.some((e) => e.count != null);
if (quotas) events.forEach((e) => {
  if (!Number.isInteger(e.count) || e.count < 0)
    throw Error(`${e.type}: when any event has "count", every event needs a whole-number "count"`);
  if (e.weight != null) throw Error(`${e.type}: use either "weight" or "count", not both`);
});
const total = quotas ? events.reduce((s, e) => s + e.count, 0) : plan.count;
if (quotas && plan.count != null && plan.count !== total)
  throw Error(`plan count ${plan.count} doesn't match the sum of event counts (${total}); remove it or fix it`);
if (!quotas && !(Number.isInteger(plan.count) && plan.count > 0))
  throw Error(`plan needs a positive "count" (or a "count" on every event)`);
const left = new Map(events.map((e) => [e, e.count]));

// ---- proto loading ----
const roots = {};
const types = {};
const typeOf = (e) => (types[e.proto + "|" + e.type] ??= (() => {
  const file = path.resolve(planDir, e.proto);
  const root = (roots[file] ??= protobuf.loadSync(file).resolveAll());
  return root.lookupType(e.type);
})());

// ---- helpers ----
const get = (o, p) => p.split(".").reduce((a, k) => a?.[k], o);
const hex = (n) => faker.string.hexadecimal({ length: n, casing: "lower", prefix: "" });
const isTs = (t) => t && t.fullName === ".google.protobuf.Timestamp";
const toTs = (d) => {
  const ms = new Date(d).getTime();
  return { seconds: Math.floor(ms / 1000), nanos: (ms % 1000) * 1e6 };
};
const fakerFn = (p) => {
  const i = p.lastIndexOf(".");
  const mod = i < 0 ? faker : get(faker, p.slice(0, i));
  const fn = mod?.[p.slice(i + 1)];
  return typeof fn === "function" ? fn.bind(mod) : null;
};

// ---- collections: datasets (filled at startup) and pools (filled by events) ----
// a collection holds records in sub-pools; without "split" it has one unnamed sub-pool ""
const DS = plan.datasets || {};
const poolDefs = { ...plan.pools };
events.forEach((e) => Object.keys(e.pools).forEach((n) => {
  if (n in DS) throw Error(`${e.type}: can't add to "${n}", it's a dataset; datasets are only filled at startup`);
  poolDefs[n] ??= {};
}));

const store = {};
function makeCollection(kind, name, split) {
  if (name.includes(".") || name.includes("/")) throw Error(`${kind} "${name}": names can't contain "." or "/"`);
  if (split && (!Object.keys(split).length || Object.values(split).some((w) => !(w > 0))))
    throw Error(`${kind} "${name}": split needs one or more sub-pools with positive shares`);
  const subs = split ? Object.keys(split) : [""];
  return { kind, name, split, subs: Object.fromEntries(subs.map((s) => [s, []])), credit: Object.fromEntries(subs.map((s) => [s, 0])) };
}
for (const [n, d] of Object.entries(DS)) store[n] = makeCollection("dataset", n, d.split);
for (const [n, d] of Object.entries(poolDefs)) {
  if (n in store) throw Error(`"${n}" is both a dataset and a pool; names must be unique`);
  store[n] = makeCollection("pool", n, d.split);
}

// "name", "name/sub", "name.prop.path" or "name/sub.prop.path"
function parseSpec(spec) {
  const dot = spec.indexOf(".");
  const key = dot < 0 ? spec : spec.slice(0, dot);
  const [name, sub] = key.split("/");
  return { key, name, sub, prop: dot < 0 ? "" : spec.slice(dot + 1) };
}
const listsOf = (key) => {
  const { name, sub } = parseSpec(key);
  return sub != null ? [store[name].subs[sub]] : Object.values(store[name].subs);
};
const sizeOf = (key) => listsOf(key).reduce((s, l) => s + l.length, 0);

// split collections deal records out in a fixed rotation, so every sub-pool gets its exact share
function addRecord(c, rec) {
  let s = "";
  if (c.split) {
    const W = Object.values(c.split).reduce((a, b) => a + b, 0);
    for (const k in c.split) c.credit[k] += c.split[k];
    s = Object.keys(c.split).reduce((a, b) => (c.credit[b] > c.credit[a] ? b : a));
    c.credit[s] -= W;
  }
  const list = c.subs[s];
  list.push(rec);
  if (c.kind === "pool" && !quotas && list.length > 10000) list.shift(); // quotas rely on every record staying available
}

let picked = {};    // one record per collection (or sub-pool) per message
function pick(spec, take) {
  const { key, prop } = parseSpec(spec);
  if (!(key in picked)) {
    const lists = listsOf(key);
    const n = lists.reduce((s, l) => s + l.length, 0);
    if (!n) return undefined;
    let i = faker.number.int({ max: n - 1 });
    const list = lists.find((l) => (i < l.length ? true : ((i -= l.length), false)));
    picked[key] = take ? list.splice(i, 1)[0] : list[i];
  }
  return prop ? get(picked[key], prop) : picked[key];
}

// ---- rules ----
const RULES = ["static", "uuid", "iso", "regex", "oneOf", "int", "email", "faker",
  "field", "traceparent", "from", "fromPool", "ref"];
const READS = new Set(["from", "fromPool", "ref"]);
const specOf = (r) => r.from ?? r.fromPool ?? r.ref;

const scalar = (t) => {
  if (t === "string") return faker.lorem.words(2);
  if (t === "bool") return faker.datatype.boolean();
  if (t === "bytes") return Buffer.from(faker.string.alphanumeric(8));
  if (/float|double/.test(t)) return faker.number.float({ max: 1000, fractionDigits: 2 });
  return faker.number.int({ min: 0, max: 100000 });
};
const mapKey = (t) => (t === "string" ? faker.word.noun() : String(scalar(t)));

function byRule(r, obj) {
  if ("static" in r) return r.static;
  if (r.uuid) return faker.string.uuid();
  if (r.iso) return (r.iso === "past" ? faker.date.past()
    : r.iso === "future" ? faker.date.future() : faker.date.recent()).toISOString();
  if (r.regex) return faker.helpers.fromRegExp(r.regex);
  if (r.oneOf) return faker.helpers.arrayElement(r.oneOf);
  if (r.int) return faker.number.int({ min: r.int[0], max: r.int[1] });
  if (r.email) return faker.internet.email();
  if (r.faker) return fakerFn(r.faker)(...(r.args || []));
  if (r.field) return get(obj, r.field);
  if (r.traceparent)
    return `00-${hex(32)}-${hex(16)}-${Math.random() < (r.traceparent.sampled ?? 1) ? "01" : "00"}`;
  if (specOf(r)) {
    const v = pick(specOf(r), r.take) ?? (r.fallback ? byRule(r.fallback, obj) : undefined);
    if (v === undefined) throw Error(`"${specOf(r)}" has no records left to read`);
    return v;
  }
  throw Error(`unknown rule ${JSON.stringify(r)}`);
}

// "unique": true regenerates until the value hasn't been used in this scope (a dataset or event field)
const seen = new Map();
function byRuleIn(scope, r, obj) {
  try {
    return uniqueByRule(scope, r, obj);
  } catch (err) {
    throw Error(`${scope}: ${err.message}`);
  }
}
function uniqueByRule(scope, r, obj) {
  if (!r.unique) return byRule(r, obj);
  const used = seen.get(scope) ?? seen.set(scope, new Set()).get(scope);
  for (let i = 0; i < 1000; i++) {
    const v = byRule(r, obj);
    const k = typeof v === "object" ? JSON.stringify(v) : v;
    if (!used.has(k)) return used.add(k), v;
  }
  throw Error(`couldn't find an unused value after 1000 tries (${used.size} used); ` +
    `the rule allows too few different values for "unique"`);
}

// ---- validation (fail fast) ----
function checkSpec(spec, where, take) {
  if (typeof spec !== "string" || !spec) throw Error(`${where}: expected a dataset or pool name, got ${JSON.stringify(spec)}`);
  const { name, sub } = parseSpec(spec);
  const c = store[name];
  if (!c) throw Error(`${where}: no dataset or pool named "${name}"`);
  const subs = c.split ? Object.keys(c.split).join(", ") : "";
  if (sub != null && !(c.split && sub in c.split))
    throw Error(`${where}: ${c.kind} "${name}" has no sub-pool "${sub}"${subs ? ` (it has ${subs})` : ""}`);
  if (take && sub == null && c.split)
    throw Error(`${where}: can't take from all of "${name}"; take from one of its sub-pools (${subs})`);
  return c;
}
// returns the rule kind; collects the keys the rule reads without a fallback into `reads`
function checkRule(r, where, reads, inDataset = false) {
  if (!r || typeof r !== "object") throw Error(`${where}: rule must be an object, got ${JSON.stringify(r)}`);
  const kind = RULES.find((k) => k in r);
  if (!kind) throw Error(`${where}: unknown rule ${JSON.stringify(r)} (expected one of ${RULES.join(", ")})`);
  if (kind === "faker" && !fakerFn(r.faker)) throw Error(`${where}: faker has no function "${r.faker}"`);
  if (READS.has(kind)) {
    checkSpec(specOf(r), where, r.take && !inDataset); // dataset sizes are counted after building
    if (reads && !r.fallback) reads.push({ key: parseSpec(specOf(r)).key, take: !!r.take });
  }
  if (r.fallback) checkRule(r.fallback, `${where} fallback`, null, inDataset);
  return kind;
}
function checkPath(type, p, where) {
  let t = type;
  for (const name of p.split(".")) {
    const f = t instanceof protobuf.Type && t.fields[name];
    if (!f) throw Error(`${where}: no field "${p}" in ${type.fullName.slice(1)}`);
    t = f.resolvedType;
  }
}

// ---- datasets: check, order by dependency, build ----
const dsDeps = {};
for (const [n, d] of Object.entries(DS)) {
  const where = `dataset "${n}"`;
  if (d.from == null && !(Number.isInteger(d.size) && d.size >= 0))
    throw Error(`${where}: needs a whole-number "size", or "from" another dataset`);
  if (d.size != null && !(Number.isInteger(d.size) && d.size >= 0)) throw Error(`${where}: "size" must be a whole number`);
  const deps = new Set();
  const dep = (spec, w) => {
    const c = checkSpec(spec, w);
    if (c.kind !== "dataset") throw Error(`${w}: datasets can only read other datasets, "${c.name}" is a pool`);
    deps.add(c.name);
  };
  if (d.from != null) {
    dep(d.from, `${where} from`);
    if (parseSpec(d.from).prop) throw Error(`${where}: "from" takes a dataset or sub-pool, not a field ("${d.from}")`);
  }
  for (const [k, r] of Object.entries(d.fields || {})) {
    const w = `${where} fields.${k}`;
    checkRule(r, w, null, true);
    for (let x = r; x; x = x.fallback) if (specOf(x)) dep(specOf(x), w);
  }
  dsDeps[n] = deps;
}
const dsOrder = [];
const state = {};
const visit = (n, trail) => {
  if (state[n] === "done") return;
  if (state[n] === "busy") throw Error(`datasets depend on each other in a loop: ${[...trail, n].join(" -> ")}`);
  state[n] = "busy";
  dsDeps[n].forEach((m) => visit(m, [...trail, n]));
  state[n] = "done";
  dsOrder.push(n);
};
Object.keys(DS).forEach((n) => visit(n, []));

for (const n of dsOrder) {
  const d = DS[n];
  let base;
  if (d.from != null) {
    const all = listsOf(parseSpec(d.from).key).flat();
    const size = d.size ?? all.length;
    if (size > all.length)
      throw Error(`dataset "${n}": wants ${size} records from "${d.from}", which only has ${all.length}`);
    base = faker.helpers.arrayElements(all, size);
  } else base = Array.from({ length: d.size }, () => ({}));
  for (const b of base) {
    picked = {};
    const rec = { ...b };
    for (const [k, r] of Object.entries(d.fields || {})) rec[k] = byRuleIn(`dataset "${n}" fields.${k}`, r, rec);
    addRecord(store[n], rec);
  }
  const c = store[n];
  const parts = c.split ? ` (${Object.entries(c.subs).map(([s, l]) => `${s} ${l.length}`).join(", ")})` : "";
  console.error(`dataset ${n}: ${sizeOf(n)} records${parts}`);
}
picked = {};

// ---- events: check against their proto ----
// needs: keys an event reads without a fallback; the event is only eligible once they have records
const needs = new Map();
const takes = new Map(); // subset of needs the event removes a record from
events.forEach((e) => {
  const Type = typeOf(e);
  const reads = [];
  for (const [p, r] of Object.entries(e.rules)) {
    checkPath(Type, p, `${e.type} rules`);
    if (checkRule(r, `${e.type} rules.${p}`, reads) === "field")
      throw Error(`${e.type} rules.${p}: "field" rules only work in headers and datasets`);
  }
  for (const [h, r] of Object.entries(e.headers)) checkRule(r, `${e.type} headers.${h}`, reads);
  if (e.key) checkPath(Type, e.key, `${e.type} key`);
  for (const [pn, add] of Object.entries(e.pools))
    for (const [k, src] of Object.entries(add)) {
      const where = `${e.type} pools.${pn}.${k}`;
      if (src.startsWith("$header.")) continue;
      if (src.startsWith("$from.")) reads.push({ key: parseSpec(src.slice(6)).key, take: false }), checkSpec(src.slice(6), where);
      else checkPath(Type, src, where);
    }
  needs.set(e, new Set(reads.map((x) => x.key)));
  takes.set(e, new Set(reads.filter((x) => x.take).map((x) => x.key)));
});

// every pool that is read needs an event that adds to it
for (const c of Object.values(store)) {
  if (c.kind !== "pool") continue;
  const readers = events.filter((e) => [...needs.get(e)].some((k) => parseSpec(k).name === c.name));
  if (readers.length && !events.some((e) => c.name in e.pools))
    throw Error(`pool "${c.name}": ${readers.map((e) => e.type).join(", ")} read from it, but no event adds to it`);
}

// with quotas, make sure every dataset or pool has enough records for the events that read it
if (quotas) {
  const list = (es) => es.map((e) => `${e.type} (${e.count})`).join(" + ");
  const sum = (es) => es.reduce((s, e) => s + e.count, 0);
  const keys = new Set(events.flatMap((e) => [...needs.get(e)]));
  for (const key of keys) {
    const { name, sub } = parseSpec(key);
    const c = store[name];
    let supply, from;
    if (c.kind === "dataset") {
      supply = sizeOf(key);
      from = `dataset "${key}" only has ${supply}`;
    } else {
      const adders = events.filter((e) => name in e.pools && e.count > 0);
      const added = sum(adders);
      const W = c.split ? Object.values(c.split).reduce((a, b) => a + b, 0) : 1;
      supply = sub != null ? Math.floor(added * c.split[sub] / W) : added;
      from = sub != null ? `${list(adders)} add ${added}, and only ${supply} of them go to "${key}"`
        : `${list(adders)} only add ${added}`;
    }
    // reading all of a split collection competes with every take from its sub-pools
    const takesKey = (e) => takes.get(e).has(key) || (sub == null && [...takes.get(e)].some((k) => parseSpec(k).name === name));
    const takers = events.filter((e) => takesKey(e) && e.count > 0);
    const readers = events.filter((e) => needs.get(e).has(key) && !takes.get(e).has(key) && e.count > 0);
    const taken = sum(takers);
    if (!supply && (takers.length || readers.length))
      throw Error(`"${key}": ${list([...takers, ...readers])} read from it, but it never gets any records`);
    if (taken > supply) throw Error(`"${key}": ${list(takers)} take ${taken} records, but ${from}`);
    if (readers.length && taken === supply)
      throw Error(`"${key}": ${list(takers)} take all ${supply} records, so ${list(readers)} may find it empty; ` +
        `add more records or take fewer`);
  }
}

// ---- generation ----
function value(f, rules, depth, p) {
  const t = f.resolvedType;
  if (rules[p]) {
    const v = byRuleIn(`${f.parent.fullName.slice(1)}.${f.name}`, rules[p]);
    return isTs(t) ? toTs(v) : v;
  }
  if (isTs(t)) return toTs(faker.date.recent());
  if (t instanceof protobuf.Enum) {
    const names = Object.keys(t.values);
    const real = names.filter((n) => !n.endsWith("_UNSPECIFIED"));
    return faker.helpers.arrayElement(real.length ? real : names);
  }
  if (t instanceof protobuf.Type) return gen(t, rules, depth + 1, p);
  return scalar(f.type);
}

function gen(type, rules, depth = 0, prefix = "") {
  const o = {};
  const pathOf = (n) => (prefix ? `${prefix}.${n}` : n);
  const ruled = (p) => Object.keys(rules).some((k) => k === p || k.startsWith(p + "."));

  const skip = new Set();
  for (const oo of type.oneofsArray) {
    const names = oo.fieldsArray.map((f) => f.name);
    const synthetic = oo.name.startsWith("_") && names.length === 1;
    const forced = names.find((n) => ruled(pathOf(n)));
    const pickName = forced ?? (synthetic && Math.random() < 0.3 ? null : faker.helpers.arrayElement(names));
    names.forEach((n) => n !== pickName && skip.add(n));
  }

  for (const f of type.fieldsArray) {
    if (skip.has(f.name)) continue;
    const p = pathOf(f.name);
    if (f.resolvedType instanceof protobuf.Type && !isTs(f.resolvedType) && depth > 3) continue;
    if (f.map) o[f.name] = { [mapKey(f.keyType)]: value(f, rules, depth, p) };
    else if (f.repeated)
      o[f.name] = Array.from({ length: faker.number.int({ min: 1, max: 3 }) }, () => value(f, rules, depth, p));
    else o[f.name] = value(f, rules, depth, p);
  }
  return o;
}

function makeMessage(i) {
  const eligible = events.filter((e) => (!quotas || left.get(e) > 0) && [...needs.get(e)].every((k) => sizeOf(k) > 0));
  if (!eligible.length) throw Error("no event can run: every remaining event needs a record from an empty dataset or pool");
  const e = faker.helpers.weightedArrayElement(
    eligible.map((x) => ({ weight: quotas ? left.get(x) : x.weight ?? 1, value: x })));
  if (quotas) left.set(e, left.get(e) - 1);
  const Type = typeOf(e);
  picked = {};

  const obj = gen(Type, e.rules);
  const msg = Type.fromObject(obj); // converts enum names, map keys, nested objects
  const err = Type.verify(msg);
  if (err) throw Error(`${e.type}: ${err}`);
  const bytes = Buffer.from(Type.encode(msg).finish());

  const headers = {};
  if (plan.typeHeader) headers[plan.typeHeader] = e.type;
  for (const [h, r] of Object.entries(e.headers)) {
    const v = byRule(r, obj);
    if (v == null) throw Error(`${e.type} header ${h}: rule produced no value`);
    headers[h] = String(v);
  }

  for (const [pn, add] of Object.entries(e.pools)) {
    const rec = {};
    for (const [k, src] of Object.entries(add))
      rec[k] = src.startsWith("$header.") ? headers[src.slice(8)]
        : src.startsWith("$from.") ? pick(src.slice(6), false)
        : get(obj, src);
    addRecord(store[pn], rec);
  }

  if (plan.outDir) {
    const dir = path.resolve(planDir, plan.outDir);
    fs.mkdirSync(dir, { recursive: true });
    const short = e.type.split(".").pop();
    fs.writeFileSync(path.join(dir, `${String(i + 1).padStart(4, "0")}-${short}.protobin`), bytes);
  }

  const k = e.key ? get(obj, e.key) : undefined;
  return { topic: e.topic, Type, bytes, msg: { key: k != null ? String(k) : null, value: bytes, headers } };
}

// ---- run ----
let stopping = false;
process.on("SIGINT", () => {
  if (stopping) process.exit(130);
  stopping = true;
  console.log("stopping after current batch (Ctrl+C again to force)");
});

(async () => {
  let producer;
  try {
    if (!plan.dryRun) {
      producer = new Kafka({ clientId: plan.clientId || "kafka-proto-gen", brokers: plan.brokers })
        .producer({ createPartitioner: Partitioners.DefaultPartitioner });
      await producer.connect();
    }
    const rate = plan.ratePerSec || total;
    let sent = 0;
    while (sent < total && !stopping) {
      const t0 = Date.now();
      const n = Math.min(rate, total - sent);
      const batch = Array.from({ length: n }, (_, j) => makeMessage(sent + j));

      if (plan.dryRun) {
        const view = { enums: String, longs: String, bytes: String };
        batch.forEach((b) => console.log(JSON.stringify({
          topic: b.topic, key: b.msg.key, headers: b.msg.headers,
          value: b.Type.toObject(b.Type.decode(b.bytes), view),
        })));
      } else {
        const byTopic = {};
        batch.forEach((b) => (byTopic[b.topic] ??= []).push(b.msg));
        await producer.sendBatch({
          topicMessages: Object.entries(byTopic).map(([topic, messages]) => ({ topic, messages })),
        });
      }

      sent += n;
      console.error(`sent ${sent}/${total}`);
      const wait = 1000 - (Date.now() - t0);
      if (!plan.dryRun && sent < total && !stopping && wait > 0) await new Promise((r) => setTimeout(r, wait));
    }
  } catch (err) {
    console.error(err.message || err);
    process.exitCode = 1;
  } finally {
    if (producer) await producer.disconnect();
  }
})();
