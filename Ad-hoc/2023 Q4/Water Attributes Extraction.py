# Databricks notebook source
# MAGIC %sql
# MAGIC select product_id, material_name, ROUND(SUM(amount),0) as sales
# MAGIC from gold.material_master t1
# MAGIC join gold.pos_transactions t2
# MAGIC on t1.material_id = t2.product_id
# MAGIC where category_name = 'WATER'
# MAGIC group by product_id, material_name
# MAGIC order by material_name

# COMMAND ----------

# i need to mine attributes from a set of material names. the output has to be in python dictionary format. i have attached a couple of sample material names with their expected attributes. Follow similar procedure for the given material names. the set of attributes are fixed as given in the sample dictionary. whenever a given attribute is not available/not clear, insert 'Not Available' at the corresponding key-value position. is this understood?

# now i'll paste the sample file based on which you'll have to provide the output dictionary.

# sample dictionary is as follows:

dct = {'GULFA MIN.WATER 330ML 24S P/O': {'volume': '330', 'units': 'ML', 'item_count': '24', 'packaging_info': 'P/O'},
       'KARDELEN MINERAL WATER 1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
       'ALAIN M/WATER CUP 48X100ML P/O': {'volume': '100', 'units': 'ML', 'item_count': '48', 'packaging_info': 'P/O'},
       'ALPIN NAT.SPRNG WTR 500ML 10+2': {'volume': '500', 'units': 'ML', 'item_count': '10+2', 'packaging_info': 'Not Available'},
       'QATARAT EMPTY BOTTLE 5GAL': {'volume': '5', 'units': 'GAL', 'item_count': '1', 'packaging_info': 'Not Available'},
       'EVIAN STILL MNRL WATER GB 750M': {'volume': '750', 'units': 'M', 'item_count': '1', 'packaging_info': 'GB'},
       'ICE SPRKL.WTR STRW&WTRMLN 17OZ': {'volume': '17', 'units': 'OZ', 'item_count': '1', 'packaging_info': 'Not Available'},
       'ALAIN SPARKL.WATER G/BTL 750ML': {'volume': '750', 'units': 'ML', 'item_count': '1', 'packaging_info': 'G/BTL'},
       'VODA NAT.MINRL WATER G/B 750ML': {'volume': '750', 'units': 'ML', 'item_count': '1', 'packaging_info': 'G/B'},
       'AQUAFINA M/WATER1.5LT 11+1FREE': {'volume': '1.5', 'units': 'LT', 'item_count': '11+1FREE', 'packaging_info': 'Not Available'},
       'FUENSNTA NAT.WATER N/CARB500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
       'GDY SPARKLING PEACH NECTAR1000': {'volume': 'Not Available', 'units': 'Not Available', 'item_count': '1', 'packaging_info': 'Not Available'},
       'ZZSAN SPARKLNG NMNRL WATER0.5L': {'volume': '0.5', 'units': 'L', 'item_count': '1', 'packaging_info': 'Not Available'},
       'VOSS WATER ASTD 375ML 3S PO': {'volume': '375', 'units': 'ML', 'item_count': '3', 'packaging_info': 'PO'},
       'A/C NAT.MINRL WATR GLSBTL 330M': {'volume': '330', 'units': 'M', 'item_count': '1', 'packaging_info': 'GLSBTL'},
       'COOL BLUE MINERAL WATER 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
       'ARWA DRINKING WATER500M 12SP/O': {'volume': '500', 'units': 'M', 'item_count': '12', 'packaging_info': 'P/O'},
       'ARWA MINRL.WATER 500ML 24S PO': {'volume': '500', 'units': 'ML', 'item_count': '24', 'packaging_info': 'PO'},
       'VOLVIC MIN.WATER 330ML 24S P/O': {'volume': '330', 'units': 'ML', 'item_count': '24', 'packaging_info': 'P/O'},
       'ALAIN MINRL.WATER S/CAP 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'S/CAP'},
       'RFW C.RCA ARTSN.WATR ALUM250ML': {'volume': '250', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
       'MSFI WTR MYLTLPONY 24X200ML PO': {'volume': '200', 'units': 'ML', 'item_count': '24', 'packaging_info': 'PO'},
       'PERRIER WATER 750ML 2S PO': {'volume': '750', 'units': 'ML', 'item_count': '2', 'packaging_info': 'PO'},
       'J/AKHDR MIN.WATER 1.5LT 12S PO': {'volume': '1.5', 'units': 'LT', 'item_count': '12', 'packaging_info': 'PO'}}

# now i'll give you the list of material names and based on which you'll give me the final dictionary. understood?

# COMMAND ----------

output_dict = {
    'M/DUBAI WATER BTL 200ML 24S PO': {'volume': '200', 'units': 'ML', 'item_count': '24', 'packaging_info': 'PO'},
    'ALAIN DRINK.WATER BTL 200M20+4': {'volume': '200', 'units': 'ML', 'item_count': '20+4', 'packaging_info': 'Not Available'},
    'M/DUBAI DRNK.WATER330ML 12S PO': {'volume': '330', 'units': 'ML', 'item_count': '12', 'packaging_info': 'PO'},
    'AL AIN DRINKING WATER1.5LT 5+1': {'volume': '1.5', 'units': 'LT', 'item_count': '5+1', 'packaging_info': 'Not Available'},
    'M/DUBAI DRNK.WATER500ML 12S PO': {'volume': '500', 'units': 'ML', 'item_count': '12', 'packaging_info': 'PO'},
    'MASAFI DRINK.WATER 1.5LT 6S PO': {'volume': '1.5', 'units': 'LT', 'item_count': '6', 'packaging_info': 'PO'},
    'ALAIN DRINKING WATER500ML 10+2': {'volume': '500', 'units': 'ML', 'item_count': '10+2', 'packaging_info': 'Not Available'},
    'ALAIN DRINKING WATER 330M 10+2': {'volume': '330', 'units': 'ML', 'item_count': '10+2', 'packaging_info': 'Not Available'},
    'M/DUBAI DRNK.WATER 1.5LT 6SP/O': {'volume': '1.5', 'units': 'LT', 'item_count': '6', 'packaging_info': 'SP/O'},
    'OASIS WATER 1.5LT 6S P/O': {'volume': '1.5', 'units': 'LT', 'item_count': '6', 'packaging_info': 'P/O'},
    'ALAIN DRINK.WATER 330ML 24S PO': {'volume': '330', 'units': 'ML', 'item_count': '24', 'packaging_info': 'PO'},
    'MSFI DRNK.WATER SHNK 24X200MPO': {'volume': '200', 'units': 'ML', 'item_count': '24', 'packaging_info': 'PO'},
    'OASIS DRINK.WATER 500ML 12S PO': {'volume': '500', 'units': 'ML', 'item_count': '12', 'packaging_info': 'PO'},
    'MASAFI DRINK.WATER 12X500ML PO': {'volume': '500', 'units': 'ML', 'item_count': '12', 'packaging_info': 'PO'},
    'ALAIN DRINK.WATER 500ML 24S PO': {'volume': '500', 'units': 'ML', 'item_count': '24', 'packaging_info': 'PO'},
    'MASAFI DRINK.WATER 12X330ML PO': {'volume': '330', 'units': 'ML', 'item_count': '12', 'packaging_info': 'PO'},
    'VOLVIC MINERAL WATER 330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MAIDUBAI DRINK.WATER BTL 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'PERRIER SPARK.WATER REG. 200ML': {'volume': '200', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'PERRIER WATER 330ML 3+1': {'volume': '330', 'units': 'ML', 'item_count': '3+1', 'packaging_info': 'Not Available'},
    'ARWA DRINKING WATER 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'OASIS WATER 330ML 12S PO': {'volume': '330', 'units': 'ML', 'item_count': '12', 'packaging_info': 'PO'},
    'EVIAN PREM.N/MIN.WATER 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MASAFI DRINKING WATER 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'PERRIER SPARK.WATER LEMON200ML': {'volume': 'Not Available', 'units': 'Not Available', 'item_count': '1', 'packaging_info': 'Not Available'},
    'EVIAN PREM.N/MIN.WATER 330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'AL AIN DRINKING WATER 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'SAFA ALAIN DRNKING WATER 330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'PERRIER SPARK.WATER REG. 330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ARWA DRINKING WATER 330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MASAFI DRNK.WATER 200ML12S P/O': {'volume': '200', 'units': 'ML', 'item_count': '12', 'packaging_info': 'P/O'},
    'ARWA DRINKING WATER 1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MAIDUBAI DRINK.WATER BTL 330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'VOLVC N/MIN.WTR BTYSHPBTL 500M': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'S.PELLEGRINO CARB.WATER 250ML': {'volume': '250', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MASAFI DRINKING WATER 1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MAIDUBAI DRINK.WATER 1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'AL AIN DRINKING WATER 1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ARWA DRINKING WATER 200ML': {'volume': '200', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MASAFI DRINKING WATER 4GAL': {'volume': 'Not Available', 'units': 'Not Available', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ACQUA PANNA MINERAL WATER250ML': {'volume': '250', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ALPIN NAT.SPRING WATER 1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MAI DUBAI DRINKING WATER 16LTR': {'volume': '16', 'units': 'LTR', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ALAIN BMBNI DRNK.WTR BTL 1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'S/PELLEGRINO CARB WATER 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ALAIN DRNK.WTR ZEROSODIUM330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MAIDUBAI DRINK.WATER BTL 200ML': {'volume': '200', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ALAIN DRNKWTR Z.SODM 6X1.5L PO': {'volume': '1.5', 'units': 'LT', 'item_count': '6', 'packaging_info': 'PO'},
    'ALAIN DRNK.WTRZEROSODM 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ACQU PANNA MINERAL WATER 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ALAIN DRNKWTR Z.SODM 12X500MPO': {'volume': '500', 'units': 'ML', 'item_count': '12', 'packaging_info': 'PO'},
    'AL AIN STILL WATER 330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'PERRIER LIME 200ML': {'volume': '200', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'M/DUBAI WATER CUP 24X200ML P/O': {'volume': '200', 'units': 'ML', 'item_count': '24', 'packaging_info': 'P/O'},
    'ALAIN DRNK.WTR ZEROSODIUM1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'LULU DRINK.WATER BTL 330ML 24S': {'volume': '330', 'units': 'ML', 'item_count': '24', 'packaging_info': 'Not Available'},
    'S/PELLEGRINO SPRKLNG WTR1000ML': {'volume': '1000', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'OASIS DRINK.WATER 200ML 12S PO': {'volume': '200', 'units': 'ML', 'item_count': '12', 'packaging_info': 'PO'},
    'FIJI ARTESIAN WATER 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ALPIN NAT.SPRING WATER 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'AL BAYAN DRINKING WATER 1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MSFI ALKLIFE ALKALINE WTR500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ZULAL DRINKNG WATER BTL 200ML': {'volume': '200', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'AL AIN DRINKING WATER 5LT P/O': {'volume': '5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'P/O'},
    'MSFI ALKLIFE ALKALINE WTR1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'EVIAN MINERAL WATER 1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'LULU DRINKING WATER 1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MAI DUBAI DRINKING WATER 5LT': {'volume': '5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'EVIAN NAT.MINRL WATER 1.5L 4+2': {'volume': '1.5', 'units': 'L', 'item_count': '4+2', 'packaging_info': 'Not Available'},
    'FIJI ARTESIAN WATER 330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ARWA DRINKING WATER500M 12SP/O': {'volume': '500', 'units': 'M', 'item_count': '12', 'packaging_info': 'P/O'},
    'MASAFI WATER ZERO SODMFR 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ALAIN DRINKING WATER ZERO200ML': {'volume': '200', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'BLU SPARKLING WATER REGULAR1LT': {'volume': '1', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'M/DUB ALKIN.ZERO SDM.WATER500M': {'volume': '500', 'units': 'M', 'item_count': '1', 'packaging_info': 'Not Available'},
    'OASIS WATER  1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'EVIAN PREM.N/MIN.WATER 1LT': {'volume': '1', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'LULU BOTTLD DRINKING WATER 5L': {'volume': '5', 'units': 'L', 'item_count': '1', 'packaging_info': 'Not Available'},
    'LULU DRINKING WATER 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MD ALKALIN ZEROSDM.WTR 12X500M': {'volume': '500', 'units': 'M', 'item_count': '12', 'packaging_info': 'Not Available'},
    'AL AIN SPARKLING WATER 330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'VOLVIC NAT.MINERAL WATER 1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'BLU SPRKL.WATER REGULAR 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ALPIN NAT.SPRING WATER 330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ARWA DRINKING WATER 1.5LT 6S': {'volume': '1.5', 'units': 'LT', 'item_count': '6', 'packaging_info': 'Not Available'},
    'MASAFI WATER ZERO SODMFR 1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'OASIS DRINKING WATER  500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ZULAL DRINKING WATER BTL 330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'PERRIER NATURAL WATER PET 1LT': {'volume': '1', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MASAFI DRINKING WATER 330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'AQUAFINA DRINK.WATER BTL 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MD ALKALIN ZEROSDM.WTR 1.5L 6S': {'volume': '1.5', 'units': 'LT', 'item_count': '6', 'packaging_info': 'Not Available'},
    'MASAFI DRINKING WATER BTL200ML': {'volume': '200', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'LULU DRINKING WATER 1.5LT 6S': {'volume': '1.5', 'units': 'LT', 'item_count': '6', 'packaging_info': 'Not Available'},
    'VOSS STILL WATER 375ML': {'volume': '375', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'SAFAALAIN WATER CUPS 200ML 30S': {'volume': '200', 'units': 'ML', 'item_count': '30', 'packaging_info': 'Not Available'},
    'AL AIN DRINKING WATER 5LT': {'volume': '5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'FIJI ARTESIAN WATER 1LT': {'volume': '1', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'M/D ALKIN.ZERO SDM.WATER1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'BLU SPRKL.WATER REGULAR 250ML': {'volume': '250', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'AQUAFINA DRINK.WATER 1.5LT 5+1': {'volume': '1.5', 'units': 'LT', 'item_count': '5+1', 'packaging_info': 'Not Available'},
    'AL AIN MINERAL WATER 1.5LT 12S': {'volume': '1.5', 'units': 'LT', 'item_count': '12', 'packaging_info': 'Not Available'},
    'AL BAYAN DRINKING WATER 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'M/DUBAI ALK.ZEROSODM WATR200ML': {'volume': '200', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MASAFI D/WATER SHRNK 24X500MPO': {'volume': '500', 'units': 'M', 'item_count': '24', 'packaging_info': 'PO'},
    'FIJI NAT. ARTESIAN WATER 700ML': {'volume': '700', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MASAFI WATER ZERO SF 6X1.5L PO': {'volume': '1.5', 'units': 'LT', 'item_count': '6', 'packaging_info': 'PO'},
    'LULU DRINK.WATER 330ML 12S': {'volume': '330', 'units': 'ML', 'item_count': '12', 'packaging_info': 'Not Available'},
    'ALPEN NAT.MINERAL WATER 200ML': {'volume': '200', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'AQUAFINA DRINKING WATER 1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'W/P SPRKL.DRNK REDGRP NAS 750M': {'volume': '750', 'units': 'M', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ALAIN BMBNI DRNK.WTR BTL 330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'AL AIN DRINKING WATER 330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'VOSS STILL WATER 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'AQFNA DRINKING WATER 500M 10+2': {'volume': '500', 'units': 'M', 'item_count': '10+2', 'packaging_info': 'Not Available'},
    'M/DUB ALKIN.ZERO SDM.WATER330M': {'volume': '330', 'units': 'M', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MSFI ALKLIFE ALKALINE WTR330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'VOSS STILL WATER 800ML': {'volume': '800', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ARWA M/WATER 330ML 12S P/O': {'volume': '330', 'units': 'ML', 'item_count': '12', 'packaging_info': 'P/O'},
    'A/PANNA MINERAL WATER 1.5LT': {'volume': '1.5', 'units': 'LT', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ALAIN SPARKL.WATER G/BTL 750ML': {'volume': '750', 'units': 'ML', 'item_count': '1', 'packaging_info': 'G/BTL'},
    'BLU SPRKL.WATER LEMON 250ML': {'volume': '250', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'LULU DRINKING WATER BTL 250ML': {'volume': '250', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MAIDUBAI DRINK.WATER CUP 100ML': {'volume': '100', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'PERRIER SPRK. WATER REG. 750ML': {'volume': '750', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'LULU DRINK.WATER BTL 200ML': {'volume': '200', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'V/WEL A/OXDNT PEACH DRNK 500ML': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'VOSS SPARKLING WATER 800ML': {'volume': '800', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'ALAIN PLUS WATR ZNCSODMFR.500M': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'AL AIN DRNK.WATER 250M20+4FREE': {'volume': '250', 'units': 'M', 'item_count': '20+4FREE', 'packaging_info': 'Not Available'},
    'EVIAN NTRL MINERAL WATER 75CL': {'volume': '75', 'units': 'CL', 'item_count': '1', 'packaging_info': 'Not Available'},
    'V/WEL CARE RED GRPFRT DRNK500M': {'volume': '500', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'MASAFI WATER ZERO SODMFR 330ML': {'volume': '330', 'units': 'ML', 'item_count': '1', 'packaging_info': 'Not Available'},
    'LARAIX DRNKNG WATRBTL 200MLX24': {'volume': '200', 'units': 'ML', 'item_count': '24', 'packaging_info': 'Not Available'},
    'W/P SPRKL.DRNK WHTGRP NAS 750M': {'volume': '750', 'units': 'M', 'item_count': '1', 'packaging_info': 'Not Available'},
    'WP SPRKLING GRAPE&PCH NAS 750M': {'volume': '750', 'units': 'M', 'item_count': '1', 'packaging_info': 'Not Available'}
}

# COMMAND ----------

import pandas as pd
df = pd.DataFrame(list(output_dict.items()), columns=['material_name', 'attributes'])

df = pd.concat([df['material_name'], df['attributes'].apply(pd.Series)], axis=1)
finaldf = spark.createDataFrame(df)
finaldf.createOrReplaceTempView('material_attributes')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from material_attributes

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


