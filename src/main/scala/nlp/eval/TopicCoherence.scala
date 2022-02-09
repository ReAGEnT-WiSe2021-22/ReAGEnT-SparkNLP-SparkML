package nlp.eval

import org.apache.spark.internal.Logging
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{DataTypes, StructType}

class TopicCoherence(override val uid: String,
                     val documentTfCol: String) extends Estimator[UMassCoherenceModel] with HasInputCols
  with HasOutputCol with Logging {

  def this(documentTfCol: String) = this(Identifiable.randomUID("topic-coherence-estimator"), documentTfCol)

  //take texts
  override def fit(dataset: Dataset[_]): UMassCoherenceModel = new UMassCoherenceModel(documentTfCol, dataset)

  override def copy(extra: ParamMap): Estimator[UMassCoherenceModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema.add(name="topicCoherence", dataType=DataTypes.DoubleType, nullable = false)
}
