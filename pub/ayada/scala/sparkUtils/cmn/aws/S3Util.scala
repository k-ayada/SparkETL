package 

import java.net.URI
import java.util.regex.Pattern
import java.io.{File,InputStream}
import java.util.{ArrayList,Properties}

import scala.collection.mutable.Buffer
import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem

import com.amazonaws.{SdkClientException,AmazonServiceException}
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.services.s3.{ AmazonS3, AmazonS3URI, AmazonS3ClientBuilder }
import com.amazonaws.services.s3.model.{S3Object, S3ObjectSummary, ObjectMetadata,Permission, GroupGrantee }
import com.amazonaws.services.s3.model.{InitiateMultipartUploadRequest, CompleteMultipartUploadRequest,AbortIncompleteMultipartUpload}
import com.amazonaws.services.s3.model.{ListObjectsRequest, GetObjectRequest, GetObjectMetadataRequest,PutObjectRequest,CopyPartRequest,CopyPartResult }
import com.amazonaws.services.s3.model.{Tag,PartETag,StorageClass,BucketLifecycleConfiguration}
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Transition;
import com.amazonaws.services.s3.model.lifecycle.{LifecycleAndOperator,LifecycleFilter,LifecyclePrefixPredicate,LifecycleTagPredicate}

import com.gwf.datalake.util.{SparkUtil,ScalaUtil}


class S3Util {
  val partSize:Long = 512 * 1024 * 1024
  private val s3Client = AmazonS3ClientBuilder
    .standard()
    .withCredentials(InstanceProfileCredentialsProvider.getInstance()).build

  /**
   * Returns the s3 client's interface
   */
  def getS3Client: AmazonS3 = s3Client

  /**
   * Converts the s3 uri String AmazonS3URI
   */
  def asS3URI(uriString: String): AmazonS3URI = {
    new AmazonS3URI(uriString)
  }

  /**
   * runs asS3URI(s"s3://$bucket/$key")
   */
  def asS3URI(bucket: String, key: String): AmazonS3URI = {
    asS3URI(s"s3://$bucket/$key")
  }

  /**
   * From the input s3 uri String, returns the bucket name and key
   */
  def getBucketNKeyFromURIStirng(uriString: String): (String, String) = {
    getBucketNKeyFromURI(asS3URI(uriString))
  }
  
  /**
   * From the input s3 uri String, returns the bucket name and key
   */
  def getBucketNKeyFromURI(uri: AmazonS3URI): (String, String) = {
    (uri.getBucket, uri.getKey)
  }
    

  /**
   *  Returns the S3 Object from the input bucket and file
   */
  def getObject(bucket: String, key: String): S3Object = {
    getS3Client.getObject(bucket, key)
  }
  /**
   *  Returns the S3 Object's meta data from the input bucket and file
   */  
  def getObjectMetadata(uriString: String): ObjectMetadata = {
      val uri = asS3URI(uriString)
      getObjectMetadata(uri.getBucket, uri.getKey)
  }
  
  def getObjectMetadata(bucket: String, key: String): ObjectMetadata = {
    getS3Client.getObjectMetadata(bucket, key)
  }

  
  /**
   *  Returns the S3 Object available in the uriString
   */
  def getObject(uriString: String): S3Object = {
    val uri = asS3URI(uriString)
    getS3Client.getObject(uri.getBucket, uri.getKey)
  }

  /**
   * Lists the top level Objects in the bucket and path
   */
  def listObjects(bucketName: String, key: String): java.util.List[String] = {
    val k = if (key.endsWith("/")) key else s"${key}/"
    val lr = new ListObjectsRequest()
      .withBucketName(bucketName)
      .withPrefix(k)
      .withDelimiter("/")
      
    getS3Client.listObjects(lr)
    .getObjectSummaries
    .asScala
    .map( o => o.getKey )
    .filter(ky => ky != k)  
    .asJava
  }

  /**
   * Lists the top level Objects in the bucket and path
   */
  def listObjects(bucketName: String, key: String, pattern: String): Buffer[S3ObjectSummary] = {
    val ptrn = Pattern.compile(pattern)
    val lr = new ListObjectsRequest()
      .withBucketName(bucketName)
      .withPrefix(if (key.endsWith("/")) key else s"${key}/")
      .withDelimiter("/")

    getS3Client
      .listObjects(lr)
      .getObjectSummaries
      .asScala
      .filter(o => ptrn.matcher(o.getKey).matches)
  }

  /**
   * Lists the top level Object keys in the bucket and path
   */
  def listObjects(uriString: String): java.util.List[String] = {

    val uri = asS3URI(uriString)
    val lr = new ListObjectsRequest()
      .withBucketName(uri.getBucket)
      .withPrefix(uri.getKey)
      .withDelimiter("/")
    getS3Client.listObjects(lr)
    .getObjectSummaries
    .asScala
    .map(o => o.getKey)
    .asJava    
  }

  /**
   * Lists the the object keys recursively
   */
  def listObjectsRecursive(uriString: String): java.util.List[String] = {

    val uri = asS3URI(uriString)
    val lr = new ListObjectsRequest()
      .withBucketName(uri.getBucket)
      .withPrefix(uri.getKey)

    getS3Client.listObjects(lr)
    .getObjectSummaries
    .asScala
    .map(o => o.getKey)
    .asJava    
  }
  /**
   * Reads the .properties file in uri and creates the Properties object
   */
  def readAsProperties(uriString: String): Properties = {
    val uri = asS3URI(uriString)
    readAsProperties(uri.getBucket, uri.getKey)
  }
  /**
   * Reads the .properties file and creates the Properties object
   */
  def readAsProperties(bucketName: String, key: String): Properties = {
    val props: Properties = new Properties()
    var s3Object: S3Object = null
    try {
      s3Object = getObject(bucketName, key)
      val stream = s3Object.getObjectContent()
      props.load(stream)
      stream.close()
    } finally {
      s3Object.close()
    }
    props
  }

  def uploadToS3(localFile: String, bucketName: String, uploadPath: String): String = {

    getS3Client.putObject(new PutObjectRequest(bucketName, uploadPath, new File(localFile)))

    //val acl = getS3Client.getObjectAcl(bucketName, uploadPath)
    //acl.grantPermission (GroupGrantee.AllUsers, Permission.Read)
    //getS3Client.setObjectAcl (bucketName, uploadPath, acl)
    s"s3:///$bucketName/$uploadPath"
  }

  /**
   * Test if there is an object @ the uriString provided.
   */
  def isKeyInS3(uriString: String): Boolean = {
    val uri = asS3URI(uriString)
    isKeyInS3(uri.getBucket, uri.getKey)
  }

  /**
   * Test if there is an object in the input bucket and key
   */
  def isKeyInS3(bucketName: String, key: String): Boolean = {
    try {
      getS3Client.getObjectMetadata(bucketName, key)
      true
    } catch {
      case e: AmazonServiceException =>
        if (e.getStatusCode == 404)
          false
        else {
          throw new Exception(e)
        }
    }
  }

  /**
   * Downloads the S3 object into the local downloadPath
   */
  def downloadFromS3(bucketName: String, key: String, downloadPath: String) {
    if (!isKeyInS3(bucketName, key)) {
      throw new RuntimeException(s"File s3://$bucketName/$key not found!")
    }
    getS3Client.getObject(new GetObjectRequest(bucketName, key), new File(downloadPath))
  }

  /**
   * Reads the objects and stores its content as a string
   */
/*  def getObjectAsString(bucketName: String, key: String): String = {
    val s3object: S3Object = getS3Client.getObject(new GetObjectRequest(bucketName, key))
    val stream: InputStream = s3object.getObjectContent();
    scala.io.Source.fromInputStream(stream).mkString
  }
  */
/**
 * Reads the objects and stores its content as a string
 */
  def getObjectAsString(bucketName: String, key: String): String = {
    if (!isKeyInS3(bucketName, key)) {
      throw new RuntimeException(s"File s3://$bucketName/key not found!")
    }
    getS3Client.getObjectAsString(bucketName, key)
  }
  
   def deletePath(hdpCfg: org.apache.hadoop.conf.Configuration, path: String, recursive: Boolean, printSummary: Boolean = true): Boolean = {

    val splits: Array[String] = path.split("/")
    val fs = FileSystem.get(new URI(s"${splits(0)}//${splits(2)}"), hdpCfg)

    val pth = new org.apache.hadoop.fs.Path(path)

    try {
      if (fs.exists(pth)) {
          SparkUtil.log(s"S3Util.deletePath(): Path found. sent delete request. Path: ${pth}")
          if (printSummary) {
          val s =fs.getContentSummary(pth)
          SparkUtil.log(s"S3Util.deletePath(): Summary: #Of Dirs: ${s.getDirectoryCount} , #Of Files: ${s.getFileCount} , Space Consumed: ${s.getSpaceConsumed} , Length ${s.getLength}")
        }                       
        fs.delete(pth, recursive)
        true
      } else {
        SparkUtil.log(s"S3Util.deletePath(): Path not found. So, it is not deleted. Path: ${pth}")
        false
      }
    } catch {
      case e: Throwable => {
        SparkUtil.log(s"S3Util.deletePath(): Failed to purge the path : ${pth}")
        SparkUtil.log(s"S3Util.deletePath(): ${ScalaUtil.getStackTraceAsStr(e)}")
        return false
      }
    }
  }
   
  def copyObject(src_bucketName: String, sourceKey: String, dst_bucketName: String, destinationKey: String){
    getS3Client.copyObject(src_bucketName,sourceKey,dst_bucketName,destinationKey)
  }
   
   def copyObject(src_bucketName: String, src_Key: String, src_Ptrn:String,dst_bucketName: String, dst_Key: String){
   val keyLst = listObjects(src_bucketName, src_Key, src_Ptrn)   
    for ( key <- keyLst) {
         SparkUtil.log(s"S3Util.copyObject() Copying:- srcBkt: ${key.getBucketName}, srcKey:${key.getKey},dstBkt: ${dst_bucketName}, dstKey:${dst_Key}" )
         val metadataRequest = new GetObjectMetadataRequest(key.getBucketName, key.getKey)
         val metadataResult = getS3Client.getObjectMetadata(metadataRequest)
         val objectSize = metadataResult.getContentLength()
         if (objectSize < 2147483648L) {
            SparkUtil.log(s"S3Util.copyObject() Copying:- Using single file mode Object copy" )
            getS3Client.copyObject(key.getBucketName,key.getKey,dst_bucketName,dst_Key)  
         } else {
            SparkUtil.log(s"S3Util.copyObject() Copying:- Using multipart mode Object copy" )
            copyObjectMultipart(key.getBucketName,key.getKey,dst_bucketName,dst_Key,objectSize)            
         }
    }
  }

  def copyObjectMultipart(srcBkt: String, srcKey: String, dstBkt: String, dstKey: String, objectSize: Long) {
    var bytePosition:Long = 0L
    var partNum = 1
    SparkUtil.log(s"copyObjectMultipart:- objectSize:${objectSize}") 
    val copyResponses = new ArrayList[CopyPartResult]()
    val initResult = s3Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(dstBkt, dstKey))
    while (bytePosition < objectSize) {
      // The last part might be smaller than partSize, so check to make sure
      // that lastByte isn't beyond the end of the object.
      val lastByte:Long = Math.min(bytePosition + partSize - 1, objectSize - 1)
      // Copy this part.
      val uid = initResult.getUploadId()
      SparkUtil.log(s"copyObjectMultipart:-\t UploadID:${uid}, partNum:${partNum}, FisrtByte:${bytePosition}, LastByte:${lastByte}, BytesToBeCopied: ${lastByte - bytePosition}")
      val copyRequest = new CopyPartRequest()
        .withSourceBucketName(srcBkt)
        .withSourceKey(srcKey)
        .withDestinationBucketName(dstBkt)
        .withDestinationKey(dstKey)
        .withUploadId(uid)
        .withFirstByte(bytePosition)
        .withLastByte(lastByte)
        .withPartNumber(partNum)
        
      copyResponses.add(s3Client.copyPart(copyRequest))
      partNum      += 1 
      bytePosition += partSize      
    }
    val completeRequest = new CompleteMultipartUploadRequest(dstBkt, dstKey,
      initResult.getUploadId(),getETags(copyResponses.asScala))

    getS3Client.completeMultipartUpload(completeRequest)
  }
  def getETags(responses: Buffer[CopyPartResult]): java.util.ArrayList[PartETag] = {
    val etags = new ArrayList[PartETag]()
    for (response <- responses) {
      etags.add(new PartETag(response.getPartNumber(), response.getETag()));
    }
    return etags;
  }

  def setExpirationDate(uriPath: String, days: Int): Boolean = {
    try {
      val uri = asS3URI(uriPath)
      val expireRule: BucketLifecycleConfiguration.Rule = new BucketLifecycleConfiguration.Rule()
        .withId(s"Expire After ${days} days")
        .withFilter(new LifecycleFilter(new LifecyclePrefixPredicate(s"${uri.getKey}/")))
        .withExpirationInDays(days)
        .withAbortIncompleteMultipartUpload({new AbortIncompleteMultipartUpload().withDaysAfterInitiation(days)})
        .withStatus(BucketLifecycleConfiguration.ENABLED)
      val configuration = new BucketLifecycleConfiguration().withRules(expireRule)
      getS3Client.setBucketLifecycleConfiguration(uri.getBucket, configuration)
      return true
    } catch {
      case e: AmazonServiceException =>
        if (e.getStatusCode == 404)
          false
        else {
          e.printStackTrace()
          return false
        }
    }
  }
  
  def getLifecycleConfs(bucket:String): java.util.List[com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule] ={
    getS3Client.getBucketLifecycleConfiguration(bucket).getRules
  }
  
}

object S3Util {
  def main(argArr: Array[String]): Unit = {
    val argMap = com.gwf.datalake.util.SparkUtil.args2map(argArr)
    val spark = SparkSession
      .builder
      .appName("S3Util")
      .enableHiveSupport()
      .getOrCreate()

    val utl = new S3Util()
    
    if (argMap.contains("--fullPath")) {
    
    val list = argMap.get("--fullPath").get.split(",")
    
      for (flPath <- list) {
        utl.deletePath(spark.sparkContext.hadoopConfiguration, s"${flPath}", true, true)
      }
    }
    
    if (argMap.contains("-purge") && argMap.contains("--bktName") && argMap.contains("--fldrs")) {
      
      val bkt = s"s3://${argMap.get("--bktName").get}"
       val list = argMap.get("--fldrs").get.split(",")
        for (fldr <- list) {
          utl.deletePath(spark.sparkContext.hadoopConfiguration, s"${bkt}/${fldr}", true, true)
      }
    }
    
    if (argMap.contains("--isKeyInS3")){
      val uriString = argMap.get("--isKeyInS3").get
      val ( bucket, key ) = utl.getBucketNKeyFromURIStirng(uriString)
      val res  = utl.isKeyInS3(bucket, key)
      SparkUtil.log(s" File URI String : ${uriString}. Bucket: ${bucket.toString}. Key: ${key.toString}. Present : ${res}")
    }
    
    
    if (argMap.contains("--listObjects")) { 
      val uriString = argMap.get("--listObjects").get
      val res  = String.join(",",utl.listObjects(uriString)) 
      
      SparkUtil.log(s" listObjects in : ${uriString}: ${res}")
    }
    
    
    if (argMap.contains("--listObjectBkt") &&  argMap.contains("--listObjectKey")&&  argMap.contains("--listObjectPtrn")) { 
      val res  = String.join(",",utl.listObjects(argMap.get("--listObjects").get
                                                ,argMap.get("--listObjectKey").get
                                                ,argMap.get("--listObjectPtrn").get
                                                ).map( o => s"${o.getKey}").toList.asJava
                           ) 
      
      SparkUtil.log(s" listObjects in : Bket: ${argMap.get("--listObjects").get} Key:${argMap.get("--listObjectKey").get} Ptrn:${argMap.get("--listObjectPtrn").get}: ${res}")
    }
    
    
    if (argMap.contains("--expirePath") && argMap.contains("--expireDays")) { 
      utl.setExpirationDate(argMap.get("--expirePath").get, argMap.get("--expireDays").get.toInt)      
      val ruleList = utl.getLifecycleConfs(utl.asS3URI(argMap.get("--expirePath").get).getBucket)
      SparkUtil.log(s" Object's Lifecycle Policies in : ${ruleList}")
    }
      
    }

    

  }
