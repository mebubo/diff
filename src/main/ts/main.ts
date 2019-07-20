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

function readCSV(path: Path): Array<Array<string>> {
    return fs.readFileSync(path.toPathString(), "utf8")
        .split(/\r?\n/)
        .map(str => str.split(","))
}

console.log(readCSV(new MyPath("./test.csv")))
