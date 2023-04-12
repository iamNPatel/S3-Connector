from connector import Connector

s3 = Connector('ml', 'parquet')

date = '2023-04-12'

# Read specific date of IA in prod
df = s3.read_from_s3('core', 'prod', 'inventory_activity', date)

# Read the last 30 days of IA in prod
df = s3.read_from_s3('core', 'prod', 'inventory_activity', date, 30,)

# read 4 specific columns for 7 days from IA
col = ['total_lead', 'prev_lead', 'current_wired_lead', 'prev_wired_lead']
df = s3.read_from_s3('core', 'prod', 'inventory_activity', date, 7, cols=col)

# read from master_data dataset (no date)
df = s3.read_from_s3('core', 'prod', 'ad_package')

# write to landing IA in dev - 50 partitions
s3.write_to_s3(df, 'landing', 'dev', 'inventory_activity', date, 50)

# write to landing IA in dev - 1 partition
s3.write_to_s3(df, 'landing', 'dev', 'inventory_activity', date)

# write to master_data dataset (no date)
s3.write_to_s3(df, 'landing', 'prod', 'ad_package')

# write to master_data dataset (no date) with partition
s3.write_to_s3(df, 'landing', 'prod', 'ad_package', partitions=50)
