WITH stg_noise_solution_survey AS (
    SELECT 
        impact_created_date,
        account_unique_id,
        participant_industry,
        uin,
        swemwbs_start_age,
        CASE
            WHEN swemwbs_start_age BETWEEN 0 and 9 THEN '0 - 9'
            WHEN swemwbs_start_age BETWEEN 10 AND 19 THEN '10-19'
            WHEN swemwbs_start_age BETWEEN 20 AND 29 THEN '20-29'
            WHEN swemwbs_start_age >= 30 THEN '30+'
            ELSE 'Unknown'
        END as swemwbs_start_age_group,
        CASE 
            WHEN gender IS NULL THEN 'Other'
            WHEN gender in ('Other', 'Prefer not', 'Non-binary') THEN 'Other'
            ELSE gender
        END as gender,
        ethnicity,
        CASE
            WHEN ethnicity IN ('White or White British',
                                'English/Welsh/Scottish/Northern Irish/British Irish',
                                'White ? British/English/Scottish/Welsh/Northern Irish',
                                'White ? any other white background') THEN 'White'

            WHEN ethnicity IN ('Black or Black British',
                                'Black/African/Caribbean/Black British ? African',
                                'White and Black Caribbean') THEN 'Black'

            WHEN ethnicity IN ('Asian',
                                'Bangladesh') THEN 'Asian'

            WHEN ethnicity IN ('Mixed',
                                'Mixed/multiple ethnic groups ? White and Black African background',
                                'White and Black Caribbean') THEN 'Mixed'

            ELSE 'Other' -- This handles any unexpected or unmatched values
        END AS ethnicity_wider,
        postcode,
        CASE
            WHEN postcode LIKE 'PE%' THEN 'Peterborough and surrounding areas'
            WHEN postcode LIKE 'IP%' THEN 'Ipswich and surrounding areas'
            WHEN postcode LIKE 'CB%' THEN 'Cambridge and surrounding areas'
            WHEN postcode LIKE 'RM%' THEN 'Romford and surrounding areas'
            WHEN postcode LIKE 'CO%' THEN 'Colchester and surrounding areas'
            WHEN postcode LIKE 'NR%' THEN 'Norwich and surrounding areas'
            WHEN postcode LIKE 'MK%' THEN 'Milton Keynes and surrounding areas'
            WHEN postcode LIKE 'SG%' THEN 'Stevenage and surrounding areas'
            WHEN postcode LIKE 'CM%' THEN 'Chelmsford and surrounding areas'
            WHEN postcode LIKE 'BH%' THEN 'Bournemouth and surrounding areas'
            WHEN postcode LIKE 'LN%' THEN 'Lincoln and surrounding areas'
            WHEN postcode LIKE 'AL%' THEN 'St Albans and surrounding areas'
            WHEN postcode LIKE 'SP%' THEN 'Salisbury and surrounding areas'
            WHEN postcode LIKE 'N%' THEN 'North London'
            ELSE 'Other'
        END AS postcode_wider,
        swemwbs_start_score,
        swemwbs_end_score,
        (swemwbs_end_score - swemwbs_start_score) as swemwbs_score_diff,
        comments,
        likes,
        posts,
        external_members
    FROM
        {{ ref('stg_noise_solution_survey') }}
)
SELECT * FROM stg_noise_solution_survey