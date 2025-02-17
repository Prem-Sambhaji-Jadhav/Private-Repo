# Databricks notebook source
nationality_dct = {'EMIRATI': ['AE', 'EMIRATI', 'UAE'],
                   'ARABS': ['ALGERIAN', 'BAHRAINI', 'IRAQI', 'JORDANIAN', 'KUWAITI', 'LEBANESE', 'MOROCCAN', 'OM', 'OMANI', 'PALESTINIAN', 'QA', 'QATARI', 'SAUDI', 'SAUDI ARABIAN', 'SYRIAN', 'TUNISIAN', 'YEMENI'],
                   'EGYPTIAN': ['EG', 'EGYPTIAN'],
                   'INDIAN': ['IN', 'INDIAN'],
                   'PAKISTANI': ['PAKISTAN', 'PAKISTANESE', 'PAKISTANI'],
                   'BANGLADESHI': ['BANGLADESHI'],
                   'SRI-LANKAN': ['SRI LANKAN', 'SRI-LANKAN', 'SRILANKA'],
                   'AFRICAN': ['CAMEROONIAN', 'ETHIOPIAN', 'GHANAIAN', 'KENYAN', 'LIBYAN', 'MAURITIAN', 'NIGERIAN', 'SOMALI', 'SOUTH AFRICAN', 'SOUTH-AFRICAN', 'SUDANESE', 'UGANDAN', 'ZIMBABWEAN'],
                   'FILIPINO': ['FILIPINO', 'PH', 'PHILIPPINE']}

nationality_groups = {'emirati': ['EMIRATI'], 'arabs_egyptians': ['ARABS', 'EGYPTIAN'], 'in_pk_bd_lk': ['INDIAN', 'PAKISTANI', 'BANGLADESHI', 'SRI-LANKAN'], 'african_filipino': ['AFRICAN', 'FILIPINO']}

# COMMAND ----------

for key, value in nationality_groups.items():
    nationality_group = nationality_groups[key]

    filter_values = []
    for nationality in nationality_group:
        filter_values = filter_values + nationality_dct[nationality]
    
    filter_values = str(filter_values)
    filter_values = filter_values.replace('[', '(')
    filter_values = filter_values.replace(']', ')')

    for i in ['material_group', 'category', 'department']:
        query = f"""
        WITH brand_sales AS (
            SELECT
                t1.store_id,
                t3.store_name,
                t2.brand,
                ROUND(SUM(t1.amount)) AS brand_revenue,
                ROW_NUMBER() OVER(PARTITION BY t1.store_id ORDER BY SUM(t1.amount) DESC) AS rank
            FROM gold.transaction.uae_pos_transactions AS t1
            JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
            JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
            JOIN gold.customer.maxxing_customer_profile AS t4 ON t1.customer_id = t4.account_key
            WHERE
                YEAR(t1.business_day) = 2024
                AND t1.store_id IN (2450, 2362)
                AND t4.nationality_desc IN {filter_values}
                AND t2.department_class_id IN (1, 2)
                AND t1.transaction_type IN ('SALE', 'SELL_MEDIA')
                AND amount > 0
                AND quantity > 0
            GROUP BY 1, 2, 3
        ),

        top_brands AS (
            SELECT
                store_id,
                store_name,
                brand,
                brand_revenue
            FROM brand_sales
            WHERE rank <= 3
            ORDER BY 1, 4 DESC
        ),

        {i}_sales AS (
            SELECT
                t1.store_id,
                t3.store_name,
                t2.brand,
                t2.{i}_id,
                t2.{i}_name,
                ROUND(SUM(t1.amount)) AS mg_revenue,
                ROW_NUMBER() OVER(PARTITION BY t1.store_id, t2.brand ORDER BY SUM(t1.amount) DESC) AS rank
            FROM gold.transaction.uae_pos_transactions AS t1
            JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
            JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
            JOIN gold.customer.maxxing_customer_profile AS t4 ON t1.customer_id = t4.account_key
            WHERE
                YEAR(t1.business_day) = 2024
                AND t1.store_id IN (2450, 2362)
                AND t4.nationality_desc IN {filter_values}
                AND t2.department_class_id IN (1, 2)
                AND CONCAT(t2.brand, t1.store_id) IN (SELECT CONCAT(brand, store_id) FROM top_brands)
                AND t1.transaction_type IN ('SALE', 'SELL_MEDIA')
                AND amount > 0
                AND quantity > 0
            GROUP BY 1, 2, 3, 4, 5
        ),

        top_{i} AS (
            SELECT
                store_id,
                store_name,
                brand,
                {i}_id,
                {i}_name,
                mg_revenue
            FROM {i}_sales
            WHERE rank <= 5
            ORDER BY 1, 6 DESC
        )

        SELECT
            t1.*,
            t2.{i}_id,
            t2.{i}_name
        FROM top_brands AS t1
        JOIN top_{i} AS t2 ON t1.store_id = t2.store_id AND t1.brand = t2.brand
        """

        df = spark.sql(query).toPandas()
        df.to_csv(f'/Workspace/Users/prem@loyalytics.in/Top Selling Brands (Ad-hoc)/top_selling_brands_{key}_{i}.csv', index = False)
