package
import org.apache.spark.sql.{ Row, Dataset }
import com.gwf.datalake.marketing.plans._
import com.gwf.datalake.util.SparkUtil

class Plans(utl : com.gwf.datalake.util.SparkUtil) {

    val select = new StringBuilder(" ")
    val from = new StringBuilder(" ")
    val MRKT = utl.db("mrkt")
    val tb_PLANS = utl.kv("--tbl-PLANS", "plans")

    val vars : scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()

    def run() : Unit = {
      
        val runAll = (utl.kv("--tstModule") == null )  // If you are commenting any module set this to false
        val buildParent = (utl.kv("-buildParent") != null)
        val dfMap = scala.collection.mutable.Map[String, Dataset[Row]]()
               
        if (runAll || utl.kv("--tstModule").contains("PlanData"))
           dfMap.put("mrktpln_pln_dim",new PlanData(utl, select, from).run)
           
        dfMap.put("mrktpln_plns", build_sk_plans)
      //utl.broadcastDF(dfMap.get("mrktpln_plns").get)
         
        if (runAll || utl.kv("--tstModule").contains("SponsorData"))
           dfMap.put("mrktpln_pln_comm_prefs",new SponsorData(utl, select, from).run)
              
        if (runAll || utl.kv("--tstModule").contains("GACampaign"))      
          dfMap.put("mrktpln_gac_ga_campaign_ele",new GACampaign(utl, select, from).run)
        
        if (runAll || utl.kv("--tstModule").contains("PrtTxnAcc"))  
          dfMap.put("mrktpln_pln_pta",new PrtTxnAcc(utl, select, from).run)
        
        if (runAll || utl.kv("--tstModule").contains("InvOpts")){
            val (mrktpln_pln_invopts,
                 mrktpln_pln_dflt_fnd_chng_dt) = new InvOpts(utl, select, from).run
          dfMap.put("mrktpln_pln_invopts",mrktpln_pln_invopts)
          dfMap.put("mrktpln_pln_dflt_fnd_chng_dt",mrktpln_pln_dflt_fnd_chng_dt)
        }
        if (runAll || utl.kv("--tstModule").contains("Funds"))  
          dfMap.put("mrktpln_pln_funds",new Funds(utl, select, from).run)
        
        if (runAll || utl.kv("--tstModule").contains("MoneyType"))  
          dfMap.put("mrktpln_pln_money_typ",new MoneyType(utl, select, from).run)
        
        if (runAll || utl.kv("--tstModule").contains("MatchRule"))  
          dfMap.put("mrktpln_pln_match_rule",new MatchRule(utl, select, from).run)

         /*Note: to derive pless_loan_ind we need both mrktpln_pln_plan_smry & mrktpln_pln_ga_srvcs
         * the select expression is in GaSrvcData.class
         */
        if (runAll || 
               (  buildParent 
                || (utl.kv("--tstModule").contains("PlanSmry") ||
                    utl.kv("--tstModule").contains("GaSrvcData")
                   )
               )     
            )   {
          dfMap.put("mrktpln_pln_plan_smry",new PlanSmry(utl, select, from).run)
          dfMap.put("mrktpln_pln_ga_srvcs",new GaSrvcData(utl, select, from).run)
        }
        if (runAll || utl.kv("--tstModule").contains("AccessCustView"))
        dfMap.put("mrktpln_pln_acc_cust_v", new AccessCustView(utl, select, from).run)
        
        if (runAll || utl.kv("--tstModule").contains("AutoRoll"))
        dfMap.put("mrktpln_pln_auto_roll", new AutoRoll(utl, select, from).run)
         
        if (runAll || utl.kv("--tstModule").contains("KeyTalk"))    
        dfMap.put("mrktpln_pln_key_talk",new KeyTalk(utl, select, from).run)

        if (runAll || utl.kv("--tstModule").contains("Recovery"))    
        dfMap.put("mrktpln_pln_recovery", new Recovery(utl, select, from).run)

        if (runAll || utl.kv("--tstModule").contains("RuleSel")) { 
        val (mrktpln_pln_instl_dist_ind,
            mrktpln_pln_loan_setup_fee) = new RuleSel(utl, select, from).run
            dfMap.put("mrktpln_pln_instl_dist_ind", mrktpln_pln_instl_dist_ind)
            dfMap.put("mrktpln_pln_loan_setup_fee", mrktpln_pln_loan_setup_fee)
        }
        if (runAll) {
          Thread.sleep(5000) //Sleep for 5 secs to make sure the temp tables are created.
    
          val plnsDF = utl.sql(s"""${select.toString}
                      ${from.toString} 
                   distribute by source_system 
                         sort by sk_plan
                   """,0)
          val finalPlan = plnsDF.drop("x")
                                .drop("y")
                                .drop("y")
                                .drop("y")
                                .drop("y")
                                .coalesce(1)
                                .sortWithinPartitions("source_system", "sk_plan")
                                          
          finalPlan.createOrReplaceTempView("finalPlan")
          utl.save2Hive(MRKT, tb_PLANS, "finalPlan", null, true) 
    
          SparkUtil.log(s"Number of plans loaded: ${finalPlan.count}")
          
    
        }
        for ( k <- dfMap.keys) {
           dfMap.get(k).get.unpersist
           if (utl.kv("--saveDFAs") == "HDFS")
                  utl.delHDFSDir(k)
        }
    }

    def build_sk_plans : Dataset[Row] = {
        utl.sql("mrktpln_plns",  utl.workTblAs , s"""
            select source_system, sk_plan, ga_id, plan_id
              from mrktpln_pln_dim
              sort by sk_plan,source_system""",1)
    }
}
