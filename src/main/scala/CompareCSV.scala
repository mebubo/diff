import cats.free.Free

object CompareCSV {

    trait FieldType

    case object StringType extends FieldType
    case object IntType extends FieldType
    case object FloatType extends FieldType
    case object BooleanType extends FieldType
    case object DateTimeType extends FieldType

    case class Field(name: String, fieldType: FieldType)

    case class Schema(fields: List[Field])

    trait Path

    trait Row

    trait Table

    trait DFrame[A] {

    }

    trait CompareF[A]

    case class Read(path: Path, fields: Schema) extends CompareF[DFrame[Row]]
    case class Join(df1: DFrame[Row], df2: DFrame[Row], key: Schema) extends CompareF[DFrame[Row]]

    type Compare[A] = Free[CompareF, A]

    // 1,foo,true
    // 2,bar,false
    val schema = Schema(List(Field("id", IntType), Field("name", StringType), Field("payed", BooleanType)))
}
