/*
 * Copyright 2021 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spline.harvester.dispatcher

import org.apache.commons.configuration.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import za.co.absa.spline.commons.annotation.Experimental
import za.co.absa.spline.commons.config.ConfigurationImplicits._
import za.co.absa.spline.commons.lang.ARM._
import za.co.absa.spline.commons.s3.SimpleS3Location.SimpleS3LocationExt
import za.co.absa.spline.harvester.dispatcher.HDFSLineageDispatcher._
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe
import za.co.absa.spline.producer.model.{ExecutionEvent, ExecutionPlan}

import java.net.URI
import scala.concurrent.blocking

@Experimental
class HDFSLineageDispatcher(filename: String, permission: FsPermission, bufferSize: Int)
  extends LineageDispatcher
    with Logging {

  def this(conf: Configuration) = this(
    filename = conf.getRequiredString(FileNameKey),
    permission = new FsPermission(conf.getRequiredObject(FilePermissionsKey).toString),
    bufferSize = conf.getRequiredInt(BufferSizeKey)
  )

  @volatile
  private var _lastSeenPlan: ExecutionPlan = _

  override def name = "HDFS"

  override def send(plan: ExecutionPlan): Unit = {
    this._lastSeenPlan = plan
  }

//  override def send(event: ExecutionEvent): Unit = {
//    if (this._lastSeenPlan == null || this._lastSeenPlan.id.get != event.planId)
//      throw new IllegalStateException("send(event) must be called strictly after send(plan) method with matching plan ID")
//
//    try {
//      // ✅ Fetch SparkContext from the active session
//      val sparkContext = SparkContext.getOrCreate()
//
//      // ✅ Read lineage directory from SparkContext config
//      val lineageDir = sparkContext.getConf.get(
//        "spark.spline.lineageDispatcher.hdfs.directory",
//        "file:///C:/tmp" // Default to local storage on Windows
//      )
//
//      // ✅ Extract executionPlanID
//      val executionPlanID = this._lastSeenPlan.id.getOrElse("unknown_plan")
//
//      // ✅ Generate a unique filename with executionPlanID
//      val timestamp = System.currentTimeMillis()
//      val path = s"$lineageDir/lineage_${executionPlanID}.json"
//
//      val planWithEvent = Map(
//        "executionPlan" -> this._lastSeenPlan,
//        "executionEvent" -> event
//      )
//
//      import HarvesterJsonSerDe.impl._
//      persistToHadoopFs(planWithEvent.toJson, path)
//    } finally {
//      this._lastSeenPlan = null
//    }
//  }

  override def send(event: ExecutionEvent): Unit = {
    if (this._lastSeenPlan == null || this._lastSeenPlan.id.get != event.planId)
      throw new IllegalStateException("send(event) must be called strictly after send(plan) method with matching plan ID")

    try {
      val sparkContext = SparkContext.getOrCreate()

      val lineageBaseDir = sparkContext.getConf.get(
        "spark.spline.lineageDispatcher.hdfs.directory",
        "file:///C:/tmp" // Default to local storage on Windows
      )

      // ✅ Extract executionPlanID
      val executionPlanID = this._lastSeenPlan.id.getOrElse("unknown_plan")

      // ✅ Extract executionPlan name
      val executionPlanName = this._lastSeenPlan.name.replaceAll("[^a-zA-Z0-9_\\-]", "_")

      // ✅ Extract executionEvent appId
      val appId = event.extra.get("appId").map(_.toString).getOrElse("unknown_appId")

      // ✅ Construct the directory path dynamically
      val fullDirPath = s"$lineageBaseDir/$executionPlanName/$appId"

      // ✅ Construct the full file path
      val filePath = s"$fullDirPath/lineage_${executionPlanID}.json"

      // ✅ Create a JSON structure
      val planWithEvent = Map(
        "executionPlan" -> this._lastSeenPlan,
        "executionEvent" -> event
      )

      import HarvesterJsonSerDe.impl._
      persistToHadoopFs(planWithEvent.toJson, filePath)
    } finally {
      this._lastSeenPlan = null
    }
  }

//  private def persistToHadoopFs(content: String, fullLineagePath: String): Unit = blocking {
//    val (fs, path) = pathStringToFsWithPath(fullLineagePath)
//
//    // Ensure the central lineage directory exists
//    val parentDir = path.getParent
//    if (!fs.exists(parentDir)) {
//      fs.mkdirs(parentDir)
//    }
//
//    logDebug(s"Opening HadoopFs output stream to $path")
//
//    val replication = fs.getDefaultReplication(path)
//    val blockSize = fs.getDefaultBlockSize(path)
//    val outputStream = fs.create(path, permission, true, bufferSize, replication, blockSize, null)
//
//    logDebug(s"Writing lineage to $path")
//    using(outputStream) {
//      _.write(content.getBytes("UTF-8"))
//    }
//  }
  private def persistToHadoopFs(content: String, fullLineagePath: String): Unit = blocking {
    val (fs, path) = pathStringToFsWithPath(fullLineagePath)

    // Ensure the entire directory structure exists
    val parentDir = path.getParent
    if (!fs.exists(parentDir)) {
      fs.mkdirs(parentDir)
    }

    logDebug(s"Opening HadoopFs output stream to $path")

    val replication = fs.getDefaultReplication(path)
    val blockSize = fs.getDefaultBlockSize(path)
    val outputStream = fs.create(path, permission, true, bufferSize, replication, blockSize, null)

    logDebug(s"Writing lineage to $path")
    using(outputStream) {
      _.write(content.getBytes("UTF-8"))
    }
  }


}

object HDFSLineageDispatcher {
  private val HadoopConfiguration = SparkContext.getOrCreate().hadoopConfiguration

  private val FileNameKey = "fileName"
  private val FilePermissionsKey = "filePermissions"
  private val BufferSizeKey = "fileBufferSize"

  /**
   * Converts string full path to Hadoop FS and Path, e.g.
   * `s3://mybucket1/path/to/file` -> S3 FS + `path/to/file`
   * `/path/on/hdfs/to/file` -> local HDFS + `/path/on/hdfs/to/file`
   *
   * @param pathString path to convert to FS and relative path
   * @return FS + relative path
   */
  def pathStringToFsWithPath(pathString: String): (FileSystem, Path) = {
    pathString.toSimpleS3Location match {
      case Some(s3Location) =>
        val s3Uri = new URI(s3Location.asSimpleS3LocationString) // s3://<bucket>
        val s3Path = new Path(s"/${s3Location.path}") // /<text-file-object-path>

        val fs = FileSystem.get(s3Uri, HadoopConfiguration)
        (fs, s3Path)

      case None => // local hdfs location
        val fs = FileSystem.get(HadoopConfiguration)
        (fs, new Path(pathString))
    }
  }
}
