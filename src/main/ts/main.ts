import fs from "fs";

enum FieldType {
    StringType,
    BooleanType,
    IntType,
    FloatType,
    DateTimeType
}

interface Path {
    toPathString(): string
}

class MyPath implements Path {
    constructor(private p: string) {}
    toPathString(): string { return this.p }
}

type Row = Map<string, string>
type Table = Array<Row>

type Schema = Array<String>

function readCSV(path: Path, schema: Schema): Table {
    return fs.readFileSync(path.toPathString(), "utf8")
        .split(/\r?\n/)
        .map(str => str.split(",").map((x, i) => [schema[i], x]))
        .map(x => new Map<string, string>(x as any))

}

interface Key {
    key(r: Row): String
}

class MyKey implements Key {
    constructor(private f: (r: Row) => String) {}
    key(r: Row): String { return this.f(r) }
}

function flatMap<A, B>(a: Array<A>, f: (a: A) => Array<B>): Array<B> {
    const b = a.map(f)
    return b[0].concat(...b.slice(1))
}

function joinMaps<K, V>(m1: Map<K, V>, m2: Map<K, V>): Map<K, V> {
    const result: Map<K, V> = new Map<K, V>();
    m1.forEach((v, k) => result.set(k, v))
    m2.forEach((v, k) => result.set(k, v))
    return result
}

function suffixKeys<V>(m: Map<string, V>, suffix: string): Map<string, V> {
    const result = new Map<string, V>();
    m.forEach((v, k) => result.set(`${k}_${suffix}`, v))
    return result
}

function join(l: Table, r: Table, key: Key): Table {
    const result: Table = [];
    l.forEach(lr => {
       r.forEach(rr => {
            if (key.key(lr) == key.key(rr)) {
                result.push(joinMaps(suffixKeys(lr, "l"), rr))
            }
       });
    });
    return result
}

const schema = ["id", "name", "payed"]
const csv1 = readCSV(new MyPath("./1.csv"), schema)
const csv2 = readCSV(new MyPath("./2.csv"), schema)

const key = (r: Row) => r.get("id") || ""

console.log(join(csv1, csv2, new MyKey(key)))

interface Column {
    type: FieldType
    name: string
}

function getInt(r: Row, c: Column): number {
    return parseInt(r.get(c.name) || "")
}

function diff(r1: Row, r2: Row, col: Column): number {
    switch (col.type) {
        case FieldType.IntType: return getInt(r1, col) - getInt(r2, col)
    }
    return 0
}

class Order {
    constructor(public id: number, public name: string, public paid: boolean) {}
}

type Order2 = {id: number, name: string, paid: boolean}

const o = new Order(1, "foo", true)
o.id = 2

interface Runnable<A, B> {
    run(a: A): B
}
