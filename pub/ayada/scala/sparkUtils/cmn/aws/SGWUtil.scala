package 

import com.amazonaws.services.storagegateway.AWSStorageGatewayAsyncClientBuilder
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.gwf.datalake.util.SparkUtil

object SGWUtil {

  def main(args: Array[String]): Unit = {
    val argMap = com.gwf.datalake.util.SparkUtil.args2map(args)
    val sgw = AWSStorageGatewayAsyncClientBuilder
      .standard()
      .withCredentials(InstanceProfileCredentialsProvider.getInstance())
      .build

       val funcs = argMap.get("--function").get

    if (funcs.contains("describeGatewayInformation")) {
      if (!argMap.contains("--gatewayARN")) {
        throw new Exception("Need value for '--gatewayARN for teh function describeGatewayInformation.'")
      }
      val sgwIReq = new com.amazonaws.services.storagegateway.model.DescribeGatewayInformationRequest()
      sgwIReq.setGatewayARN(argMap.get("--gatewayARN").get)
      SparkUtil.log(s"SGWUtil: describeGatewayInformation()\n\t\t${sgw.describeGatewayInformation(sgwIReq).toString().replace(",", "\n\t\t")}")
    }

    if (funcs.contains("describeCache")) {
      if (!argMap.contains("--gatewayARN")) {
        throw new Exception("Need value for '--gatewayARN for teh function describeCache.'")
      }
      val dscReq = new com.amazonaws.services.storagegateway.model.DescribeCacheRequest()
      dscReq.setGatewayARN(argMap.get("--gatewayARN").get)
      SparkUtil.log(s"SGWUtil: describeCache()\n\t\t${sgw.describeCache(dscReq).toString().replace(",", "\n\t\t")}")
    }

    if (funcs.contains("refreshCache")) {
       if (!argMap.contains("--fileShareARN")) {
        throw new Exception("Need value for '--fileShareARN for refreshCache'")
       }
       
       val dscReq =  if (argMap.contains("--gatewayARN")) {
           new com.amazonaws.services.storagegateway.model.DescribeCacheRequest()
       } else {
         null
       }
       
       if (dscReq != null ) {
         dscReq.setGatewayARN(argMap.get("--gatewayARN").get)
         SparkUtil.log(s"SGWUtil: describeCache()\n\t\t${sgw.describeCache(dscReq).toString().replace(",", "\n\t\t")}")
       }

       SparkUtil.log(s"SGWUtil: Refreshing cache for '${argMap.get("--fileShareARN").get}'")
       val req = new com.amazonaws.services.storagegateway.model.RefreshCacheRequest()
       req.setFileShareARN(argMap.get("--fileShareARN").get)
       sgw.refreshCache(req)
       
       if (dscReq != null ) {
         dscReq.setGatewayARN(argMap.get("--gatewayARN").get)
         SparkUtil.log(s"SGWUtil: describeCache()\n\t\t${sgw.describeCache(dscReq).toString().replace(",", "\n\t\t")}")
       }
    }
  }
}
