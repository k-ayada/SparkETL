package com.gwf.datalake.marketing.plans

import org.apache.spark.sql.{ Row, Dataset }

class GACampaign(utl : com.gwf.datalake.util.SparkUtil, select : StringBuilder, from : StringBuilder) {
    val EASY = utl.db("easy")
	  //val WORK = utl.db("work")
    val runMode = utl.kv("--runMode", "p")

    def run : Dataset[Row] = {
        
        val mrktpln_gac_easy_ga_campaign_ele = read_easy_GA_Campaign_Election
        val mrktpln_pln_ga_campaign_ele = cre_ga_campaign_ele
        
        mrktpln_gac_easy_ga_campaign_ele.unpersist
        
        select.append(s"""
                 , if (nvl(gac.spon_pref_your_ret ,0) = 1  , 1 , 0) as spon_pref_your_ret
                 , if (nvl(gac.spon_pref_tips     ,0) = 1  , 1 , 0) as spon_pref_tips
                 , if (nvl(gac.spon_pref_othr_ways,0) = 1  , 1 , 0) as spon_pref_othr_ways
                 , if (nvl(gac.spon_pref_default  ,0) = 1  , 1 , 0) as spon_pref_default   
        """)
        from.append(s"""\n  left outer join mrktpln_pln_ga_campaign_ele gac
                              on gac.source_system = plans.source_system
                             and gac.ga_id         = plans.ga_id""")

        mrktpln_pln_ga_campaign_ele

    }  
  
  def read_easy_GA_Campaign_Election : Dataset[Row] = {
        utl.sql("mrktpln_gac_easy_ga_campaign_ele", "NONE", s"""
            select source_system,ga_id, upper(status_code) as status_code,campaign_content_cat_code,default_indicator
              from ${EASY}.ga_campaign_election
             where termdate is null
        """)
    }
    //Jira:CORE-24660 update the status code compare value from 'on' to ON'
    def cre_ga_campaign_ele : Dataset[Row] = {
        utl.sql("mrktpln_pln_ga_campaign_ele",  utl.workTblAs , s"""
            select gac.ga_id,gac.source_system
                 , max(if(gac.status_code = 'ON' and gac.campaign_content_cat_code = 'Your Retirement'   , 1, 0)) as spon_pref_your_ret 
                 , max(if(gac.status_code = 'ON' and gac.campaign_content_cat_code = 'Tips & News'       , 1, 0)) as spon_pref_tips
                 , max(if(gac.status_code = 'ON' and gac.campaign_content_cat_code = 'Other Ways to Save', 1, 0)) as spon_pref_othr_ways
                 , max(if(gac.default_indicator='Y'                                                      , 1, 0)) as spon_pref_default
              from mrktpln_gac_easy_ga_campaign_ele gac
             group by gac.ga_id,gac.source_system
        """)
    }
}
