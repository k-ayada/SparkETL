package com.gwf.datalake.util.aws

import java.util.ArrayList
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.services.simplesystemsmanagement.{ AWSSimpleSystemsManagement, AWSSimpleSystemsManagementClientBuilder }
import com.amazonaws.services.simplesystemsmanagement.model.{ Parameter, ParameterMetadata, ParameterType }
import com.amazonaws.services.simplesystemsmanagement.model.{ GetParameterRequest, DeleteParametersRequest, PutParameterRequest }
import com.amazonaws.services.simplesystemsmanagement.model.{ DescribeParametersResult, DescribeParametersRequest }

import scala.collection.JavaConverters._
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersRequest
import java.io.StringReader
import java.util.Properties

class SSMUtil {

  /**
   * Returns the SSM client created by the default client
   */
  def getDefaultSSMClient: AWSSimpleSystemsManagement = {
    AWSSimpleSystemsManagementClientBuilder
      .defaultClient
  }

  /**
   * Returns all the keys available in the Parameter store
   */
  private def getAllParameterNames(): Array[String] = {
    val parameterMetadata: ArrayList[ParameterMetadata] = new ArrayList[ParameterMetadata]()
    val ssm = getDefaultSSMClient
    var nextToken: String = null
    do {
      val result: DescribeParametersResult = ssm.describeParameters(new DescribeParametersRequest().withNextToken(nextToken))
      nextToken = result.getNextToken
      parameterMetadata.addAll(result.getParameters)
    } while (nextToken != null);
    parameterMetadata.iterator().asScala.map(x => x.getName).toArray
  }

  /**
   * Stores the parameter key value pair in the parameter store
   */
  def storeParameter(parmName: String, parmValue: String, overwrite: Boolean = true, parmKMSKey: String = null): Boolean = {
    try {
      var pr = new PutParameterRequest()
        .withName(parmName)
        .withValue(parmValue)
        .withOverwrite(overwrite)
        
        if (parmKMSKey != null)
          pr = pr.withKeyId(parmKMSKey)
        
      getDefaultSSMClient.putParameter(pr)
      true
    } catch {
      case t: Throwable =>
        false
    }

    true
  }

  /**
   * Deletes the input parameter from the parameter store
   */
  def deleteParameter(parmName: String): Boolean = {
    try {
      getDefaultSSMClient.deleteParameters(new DeleteParametersRequest().withNames(parmName))
      true
    } catch {
      case t: Throwable =>
        false
    }
  }

  /**
   * Returns the string value stored in the parameter store
   */
  def getParameter(parmName: String, encrypt: Boolean = false): String = {
    try {
     val p = {
       if (parmName.startsWith("ssm://")) parmName.split("ssm:/")(1) 
       else parmName
     }
      
      getDefaultSSMClient
        .getParameters(new GetParametersRequest().withNames(p).withWithDecryption(encrypt))
        .getParameters()
        .get(0)
        .getValue
    } catch {
      case t: Throwable =>
        throw new Exception(t)
    }
  }
  /**
   * Returns the Properties object loaded using the parameter
   */
  def getParameterAsProps(parmName: String, encrypt: Boolean = false): Properties = {
    val p = new Properties()    
    p.load(new StringReader(getParameter(parmName, encrypt)))
    p
  }
  

}
