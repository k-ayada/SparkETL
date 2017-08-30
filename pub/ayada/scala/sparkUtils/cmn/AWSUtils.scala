package pub.ayada.scala.sparkUtils.cmn

import java.net.URI
import java.util.Properties

import scala.io.{ BufferedSource, Source }

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.model.GetObjectRequest

object AWSUtils {

    /**
      * pub.ayada.scala.sparkUtils.cmn.AWSUtils.isS3FileExists(s3Protocol : String, s3Bucket : String, s3File : String, sparkContext : SparkContext) : Boolean = {
      */
    def isS3FileExists(s3Protocol : String, s3Bucket : String, s3File : String, sparkContext : SparkContext) : Boolean = {
        org.apache.hadoop.fs.FileSystem.get(new URI(s3Protocol + s3Bucket), sparkContext.hadoopConfiguration).exists(new org.apache.hadoop.fs.Path(s3Protocol + s3Bucket + s3File))
    }

    /**
      * pub.ayada.scala.sparkUtils.cmn.AWSUtils.mergeSmallS3Files(sqlContext : SQLContext, s3InDirs : Array[String], s3OutDir : String, numOutputFiles : Int) = {
      */
    def mergeSmallS3Files(sqlContext : SQLContext, s3InDirs : Array[String], s3OutDir : String, numOutputFiles : Int) = {
        val dfSeq = scala.collection.mutable.MutableList[org.apache.spark.sql.DataFrame]()
        s3InDirs.map(dir => {
            dfSeq += sqlContext.read.parquet(dir)
        })
        val masterDf = dfSeq.reduce((df1, df2) => df1.unionAll(df2))
        masterDf.coalesce(numOutputFiles).write.mode(SaveMode.Append).parquet(s3OutDir)

    }
    /**
      * pub.ayada.scala.sparkUtils.cmn.AWSUtils.mergeSmallS3Files(sqlContext : SQLContext, s3InDirs : Array[String], s3OutDir : String, numOutputFiles : Int, partitionColumns : Array[String])
      */
    def mergeSmallS3Files(sqlContext : SQLContext, s3InDirs : Array[String], s3OutDir : String, numOutputFiles : Int, partitionColumns : Array[String]) = {
        val dfSeq = scala.collection.mutable.MutableList[org.apache.spark.sql.DataFrame]()
        s3InDirs.map(dir => {
            dfSeq += sqlContext.read.parquet(dir)
        })
        val masterDf = dfSeq.reduce((df1, df2) => df1.unionAll(df2))
        masterDf.coalesce(numOutputFiles).write.partitionBy(partitionColumns : _*).mode(SaveMode.Append).parquet(s3OutDir)
    }

    /**
      * pub.ayada.scala.sparkUtils.cmn.AWSUtils.getCredentialsFromAWSProfile()
      */
    def getCredentialsFromAWSProfile() : AWSCredentials = {
        new com.amazonaws.auth.profile.ProfileCredentialsProvider().getCredentials
    }
    /**
      * pub.ayada.scala.sparkUtils.cmn.AWSUtils.getAWSCredentials(accessKey : String, secretKey : String)
      */
    def getAWSCredentials(accessKey : String, secretKey : String) : AWSCredentials = {
        new com.amazonaws.auth.BasicAWSCredentials(accessKey, secretKey);
    }
    /**
      * pub.ayada.scala.sparkUtils.cmn.AWSUtils.getAWSCredentials(accessKey : String, secretKey : String)
      */
    def getAWSClient(accessKey : String, secretKey : String) : AmazonS3Client = {
        new AmazonS3Client(getAWSCredentials(accessKey, secretKey))
    }
    /**
      * pub.ayada.scala.sparkUtils.cmn.AWSUtils.getAWSCredentials(accessKey : String, secretKey : String)
      */
    def getAWSClient() : AmazonS3Client = {
        new AmazonS3Client(getCredentialsFromAWSProfile)
    }
    /**
      * pub.ayada.scala.sparkUtils.cmn.AWSUtils.readPropsFile(filePath : String)
      */
    def readPropsFile(filePath : String) : Properties = {
        val props : Properties = new Properties()
        var s3Object : S3Object = null
        try {
            s3Object = getS3Object(getAWSClient, filePath)
            val stream = s3Object.getObjectContent()
            props.load(stream)
            stream.close()
        } finally {
            s3Object.close()
        }
        props
    }
    /**
      * pub.ayada.scala.sparkUtils.cmn.AWSUtils.readPropsFile(filePath : String, accessKey : String, secretKey : String)
      */
    def readPropsFile(filePath : String, accessKey : String, secretKey : String) : Properties = {
        val props : Properties = new Properties()
        var s3Object : S3Object = null
        try {
            s3Object = getS3Object(getAWSClient(accessKey, secretKey), filePath)
            val stream = s3Object.getObjectContent()
            props.load(stream)
            stream.close()
        } catch {
            case e : Throwable => println(pub.ayada.scala.utils.ExceptionUtil.printStackTrace(e))
        } finally {
            if (s3Object != null)
                s3Object.close()
        }
        props
    }
    /**
      * pub.ayada.scala.sparkUtils.cmn.AWSUtils.getS3Object(s3Client : AmazonS3Client, filePath : String)
      */
    def getS3Object(s3Client : AmazonS3Client, filePath : String) : S3Object = {
        val uri = getS3URI(filePath)
        s3Client.getObject(uri.getBucket, uri.getKey)
    }
    /**
      * pub.ayada.scala.sparkUtils.cmn.AWSUtils.getS3URI(filePath : String)
      */
    def getS3URI(filePath : String) : AmazonS3URI = {
        new AmazonS3URI(filePath)
    }

}
