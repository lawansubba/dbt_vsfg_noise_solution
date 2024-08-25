select
     STR_TO_DATE(impact_created_date, '%m/%d/%Y') AS impact_created_date,
     CAST(account_unique_id AS char(10)) AS account_unique_id,
     CAST(participant_industry AS char(20)) AS participant_industry,
     CAST(uin AS UNSIGNED) AS uin,
     CAST(swemwbs_start_age AS UNSIGNED) AS swemwbs_start_age,
     CAST(gender AS char(10)) AS gender,
     CAST(ethnicity AS char(20)) AS ethnicity,
     CAST(postcode AS char(10)) AS postcode,
     CAST(swemwbs_start_score AS double) AS swemwbs_start_score,
     CAST(swemwbs_end_score AS double) AS swemwbs_end_score,
     CAST(comments AS UNSIGNED) AS comments,
     CAST(likes AS UNSIGNED) AS likes,
     CAST(posts AS UNSIGNED) AS posts,
     CAST(external_members AS UNSIGNED) AS external_members
from
    {{ ref('noise_solution') }}
    

